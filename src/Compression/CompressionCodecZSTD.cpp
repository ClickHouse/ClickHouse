#include <Compression/CompressionCodecZSTD.h>
#include <Compression/CompressionInfo.h>
#include <Compression/CompressionFactory.h>
#include <Compression/registerCompressionCodecs.h>
#include <zstd.h>
#include <Common/CurrentMemoryTracker.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <Common/MemoryTracker.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/PerCPU.h>
#include <Common/getNumberOfCPUCoresToUse.h>
#include <Common/memory.h>
#include <base/unit.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>
#include <IO/WriteHelpers.h>
#include <IO/ZstdContext.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <vector>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_COMPRESS;
    extern const int CANNOT_DECOMPRESS;
    extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
    extern const int ILLEGAL_CODEC_PARAMETER;
}

uint8_t CompressionCodecZSTD::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::ZSTD);
}

void CompressionCodecZSTD::updateHash(SipHash & hash) const
{
    getCodecDescription()->updateTreeHash(hash, /*ignore_aliases=*/ true);
}

UInt32 CompressionCodecZSTD::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    return static_cast<UInt32>(ZSTD_compressBound(uncompressed_size));
}


namespace
{

/// A new `ZSTD` context allocates about 650 KiB at level 3 and zeroes a large part of it. A context keeps no state that affects
/// the next block once it is reset, so the contexts are shared by all queries and tables through process-wide per-CPU pools.
/// When the pool of the current CPU is empty, an idle context is taken from the pool of another CPU before a new one is created:
/// threads roam over CPUs, and otherwise every CPU a thread happens to run on would keep its own resident context.
template <typename ContextPtr>
struct ContextPools
{
    struct Pool
    {
        std::mutex mutex;
        std::vector<std::unique_ptr<ContextPtr>> idle; /// STYLE_CHECK_ALLOW_STD_CONTAINERS: process-wide, not charged to a query
    };

    /// Returns a context to the pool of the CPU that took it, which is not necessarily the pool it was taken from.
    struct Deleter
    {
        Pool * pool = nullptr;

        void operator()(ContextPtr * context) const
        {
            std::lock_guard lock(pool->mutex);
            pool->idle.emplace_back(context);
            ++num_idle;
        }
    };

    using Holder = std::unique_ptr<ContextPtr, Deleter>;

    /// Number of idle contexts in all pools, so that no pool is locked when there are none.
    static inline std::atomic<size_t> num_idle = 0;
};

template <typename ContextPtr, typename Factory>
typename ContextPools<ContextPtr>::Holder getPooledContext(Factory && create)
{
    using Pools = ContextPools<ContextPtr>;
    static const size_t num_pools = PerCPU::getNumPossibleCPUs();
    static auto * pools = new typename Pools::Pool[num_pools];
    const size_t current = static_cast<size_t>(PerCPU::getCurrentCPU()) % num_pools;
    /// A context taken from another pool goes back to the pool of the current CPU, so it is searched for only once after
    /// a thread moves to another CPU. The counter may be stale, which costs at most one fruitless search or one new context.
    for (size_t i = 0; i < num_pools && Pools::num_idle.load(std::memory_order_relaxed) > 0; ++i)
    {
        auto & pool = pools[(current + i) % num_pools];
        std::lock_guard lock(pool.mutex);
        if (!pool.idle.empty())
        {
            ContextPtr * context = pool.idle.back().release();
            pool.idle.pop_back();
            --Pools::num_idle;
            return {context, {&pools[current]}};
        }
    }
    return {create(), {&pools[current]}};
}

/// Memory of a context as the memory tracker counts it: the holder, the context from `ZSTD_createCCtx` and the workspace
/// that `ZSTD_compress2` allocates, each rounded up by the allocator.
Int64 trackedSize([[maybe_unused]] ZSTD_CCtx * cctx)
{
#if USE_JEMALLOC
    static const size_t context_size = ZSTD_sizeof_CCtx(ZstdCCtxPtr(ZSTD_createCCtx()).get());
    static const Int64 fixed_size
        = ::Memory::getActualAllocationSize(sizeof(ZstdCCtxPtr)) + ::Memory::getActualAllocationSize(context_size);
    size_t workspace_size = ZSTD_sizeof_CCtx(cctx) - context_size;
    return fixed_size + (workspace_size ? ::Memory::getActualAllocationSize(workspace_size) : 0);
#else
    /// Without jemalloc, the allocations of `libzstd` are not tracked: only the holder from `new` is.
    return sizeof(ZstdCCtxPtr);
#endif
}

/// Charges the query for a pooled context while it uses it; the global tracker already counts the pool, so it takes the charge back.
void chargeQuery(Int64 bytes)
{
    std::ignore = bytes > 0 ? CurrentMemoryTracker::allocNoThrow(bytes) : CurrentMemoryTracker::free(-bytes);
    MemoryTrackerBlockerInThread blocker(VariableContext::Global);
    total_memory_tracker.adjustWithUntrackedMemory(-bytes);
}

}

UInt32 CompressionCodecZSTD::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    static std::atomic<size_t> num_contexts = 0;
    /// Returning a context to its pool happens in a destructor, which must not throw `MEMORY_LIMIT_EXCEEDED`.
    LockMemoryExceptionInThread lock(VariableContext::Global);
    bool created = false;
    auto holder = getPooledContext<ZstdCCtxPtr>([&]
    {
        ZstdCCtxPtr ctx(ZSTD_createCCtx());
        if (!ctx)
            throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress with ZSTD codec: failed to create compression context");
        ++num_contexts;
        created = true;
        return new ZstdCCtxPtr(std::move(ctx));
    });
    ZSTD_CCtx * cctx = holder->get();
    /// A new context is charged to the query by its own allocations.
    if (!created)
        chargeQuery(trackedSize(cctx));
    /// The previous user may have set other parameters or failed mid-frame.
    ZSTD_CCtx_reset(cctx, ZSTD_reset_session_and_parameters);
    ZSTD_CCtx_setParameter(cctx, ZSTD_c_compressionLevel, level);
    if (enable_long_range)
    {
        ZSTD_CCtx_setParameter(cctx, ZSTD_c_enableLongDistanceMatching, 1);
        ZSTD_CCtx_setParameter(cctx, ZSTD_c_windowLog, window_log); // NB zero window_log means "use default" for libzstd
    }
    size_t compressed_size = ZSTD_compress2(cctx, dest, ZSTD_compressBound(source_size), source, source_size);
    /// Pool at most two contexts per usable core (threads may roam over more CPUs) and none with a large workspace.
    if (num_contexts > 2 * getNumberOfCPUCoresToUse() || ZSTD_sizeof_CCtx(cctx) > 2_MiB)
    {
        delete holder.release();
        --num_contexts;
    }
    else
        chargeQuery(-trackedSize(cctx));

    if (ZSTD_isError(compressed_size))
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress with ZSTD codec: {}", ZSTD_getErrorName(compressed_size));

    return static_cast<UInt32>(compressed_size);
}


UInt32 CompressionCodecZSTD::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    static std::atomic<size_t> num_contexts = 0;
    MemoryTrackerBlockerInThread blocker;
    LockMemoryExceptionInThread lock(VariableContext::Global);
    auto holder = getPooledContext<ZstdDCtxPtr>([]
    {
        ZstdDCtxPtr ctx(ZSTD_createDCtx());
        if (!ctx)
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress ZSTD-encoded data: failed to create decompression context");
        ++num_contexts;
        return new ZstdDCtxPtr(std::move(ctx));
    });
    size_t res = ZSTD_decompressDCtx(holder->get(), dest, uncompressed_size, source, source_size);
    /// Same bound as for compression, so that a burst of concurrent reads does not stay resident in the pools.
    if (num_contexts > 2 * getNumberOfCPUCoresToUse())
    {
        delete holder.release();
        --num_contexts;
    }

    if (ZSTD_isError(res))
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress ZSTD-encoded data: {}", std::string(ZSTD_getErrorName(res)));
    return static_cast<UInt32>(res);
}

CompressionCodecZSTD::CompressionCodecZSTD(int level_, int window_log_)
    : level(level_)
    , enable_long_range(true)
    , window_log(window_log_)
{
}

CompressionCodecZSTD::CompressionCodecZSTD(int level_)
    : level(level_)
    , enable_long_range(false)
    , window_log(0)
{
}

ASTPtr CompressionCodecZSTD::getCodecDescription() const
{
    ASTs arguments{make_intrusive<ASTLiteral>(static_cast<UInt64>(level))};
    if (enable_long_range)
        arguments.push_back(make_intrusive<ASTLiteral>(static_cast<UInt64>(window_log)));
    return makeCodecDescription("ZSTD", arguments);
}

void registerCodecZSTD(CompressionCodecFactory & factory)
{
    UInt8 method_code = static_cast<UInt8>(CompressionMethodByte::ZSTD);
    factory.registerCompressionCodec("ZSTD", method_code, [&](const ASTPtr & arguments) -> CompressionCodecPtr {
        int level = CompressionCodecZSTD::ZSTD_DEFAULT_LEVEL;
        if (arguments && !arguments->children.empty())
        {
            if (arguments->children.size() > 2)
                throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE, "ZSTD codec must have 1 or 2 parameters, given {}",
                    arguments->children.size());

            const auto children = arguments->children;
            const auto * literal = children[0]->as<ASTLiteral>();
            if (!literal)
                throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "ZSTD codec argument must be integer");

            level = static_cast<int>(literal->value.safeGet<UInt64>());
            if (level > ZSTD_maxCLevel())
            {
                throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER,
                    "ZSTD codec can't have level more than {}, given {}",
                    ZSTD_maxCLevel(), level);
            }
            if (arguments->children.size() > 1)
            {
                const auto * window_literal = children[1]->as<ASTLiteral>();
                if (!window_literal)
                    throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "ZSTD codec second argument must be integer");

                const int window_log = static_cast<int>(window_literal->value.safeGet<UInt64>());

                ZSTD_bounds window_log_bounds = ZSTD_cParam_getBounds(ZSTD_c_windowLog);
                if (ZSTD_isError(window_log_bounds.error))
                    throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "ZSTD windowLog parameter is not supported {}",
                        std::string(ZSTD_getErrorName(window_log_bounds.error)));
                // 0 means "use default" for libzstd
                if (window_log != 0 && (window_log > window_log_bounds.upperBound || window_log < window_log_bounds.lowerBound))
                    throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER,
                                    "ZSTD codec can't have window log more than {} and lower than {}, given {}",
                                    toString(window_log_bounds.upperBound),
                                    toString(window_log_bounds.lowerBound), toString(window_log));

                return std::make_shared<CompressionCodecZSTD>(level, window_log);
            }
        }
        return std::make_shared<CompressionCodecZSTD>(level);
    });
}

CompressionCodecPtr getCompressionCodecZSTD(int level)
{
    return std::make_shared<CompressionCodecZSTD>(level);
}

}
