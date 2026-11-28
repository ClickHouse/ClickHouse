#include <Compression/CompressionCodecZXC.h>
#include <Compression/CompressionInfo.h>
#include <Compression/CompressionFactory.h>
#include <Compression/registerCompressionCodecs.h>
#include <Common/CurrentMemoryTracker.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <Common/MemoryTracker.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/PerCPU.h>
#include <Common/ServerOwnedMemory.h>
#include <Common/getNumberOfCPUCoresToUse.h>
#include <Common/memory.h>
#include <base/unit.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>

#include <algorithm>
#include <atomic>
#include <bit>
#include <memory>
#include <mutex>
#include <new>
#include <vector>

#include <zxc.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int CANNOT_COMPRESS;
    extern const int CANNOT_DECOMPRESS;
    extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
    extern const int ILLEGAL_CODEC_PARAMETER;
}

namespace
{

/// zxc contexts are carved in workspaces allocated here (static context API) instead of per call,
/// and idle ones are pooled process-wide by block size and CPU, as for `ZSTD`.

constexpr std::align_val_t workspace_alignment{64}; /// zxc requires a cache-line aligned workspace.
constexpr size_t num_block_sizes = ZXC_BLOCK_SIZE_MAX_LOG2 - ZXC_BLOCK_SIZE_MIN_LOG2 + 1;

/// Larger compression contexts (levels 6 and 7 on big blocks) are not pooled.
constexpr size_t max_pooled_compression_workspace = 4_MiB;

/// Smallest valid zxc block size holding the whole input. The decoder reads it from the frame header.
size_t blockSizeFor(UInt32 source_size)
{
    return std::clamp<size_t>(std::bit_ceil(static_cast<size_t>(source_size)), ZXC_BLOCK_SIZE_MIN, ZXC_BLOCK_SIZE_MAX);
}

bool isValidBlockSize(size_t block_size)
{
    return std::has_single_bit(block_size) && block_size >= ZXC_BLOCK_SIZE_MIN && block_size <= ZXC_BLOCK_SIZE_MAX;
}

size_t blockSizeIndex(size_t block_size)
{
    chassert(isValidBlockSize(block_size));
    return static_cast<size_t>(std::countr_zero(block_size)) - ZXC_BLOCK_SIZE_MIN_LOG2;
}

class Workspace
{
public:
    explicit Workspace(size_t size_) : size(size_), data(::operator new(size_ ? size_ : 1, workspace_alignment)) {}
    ~Workspace() { ::operator delete(data, size ? size : 1, workspace_alignment); }

    Workspace(const Workspace &) = delete;
    Workspace & operator=(const Workspace &) = delete;

    void * get() const { return data; }
    size_t bytes() const { return size; }

    Int64 trackedSize() const { return ::Memory::getActualAllocationSize(size ? size : 1, workspace_alignment); }

private:
    const size_t size;
    void * const data;
};

struct CompressionContext
{
    CompressionContext(size_t block_size_, int level_)
        : block_size(block_size_)
        , carved_level(level_)
        , current_level(level_)
        , workspace(zxc_static_cctx_workspace_size(block_size_, level_))
        , cctx(zxc_init_static_cctx(workspace.get(), workspace.bytes(), block_size_, level_))
    {
        if (!cctx)
            throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY,
                "Cannot create a ZXC compression context for blocks of {} bytes at level {}", block_size, carved_level);
    }

    /// A workspace carved below `ZXC_LEVEL_DENSITY` lacks the optimal-parser scratch.
    bool canCompressAt(int level) const { return level < ZXC_LEVEL_DENSITY || carved_level >= ZXC_LEVEL_DENSITY; }

    Int64 trackedSize() const { return ::Memory::getActualAllocationSize(sizeof(*this)) + workspace.trackedSize(); }

    const size_t block_size;
    const int carved_level;
    int current_level;
    Workspace workspace;
    /// Points inside `workspace`.
    zxc_cctx * const cctx;
};

struct DecompressionContext
{
    explicit DecompressionContext(size_t block_size_)
        : block_size(block_size_)
        , workspace(zxc_static_dctx_workspace_size(block_size_))
        , dctx(zxc_init_static_dctx(workspace.get(), workspace.bytes(), block_size_))
    {
        if (!dctx)
            throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Cannot create a ZXC decompression context for blocks of {} bytes", block_size);
    }

    const size_t block_size;
    Workspace workspace;
    /// Points inside `workspace`.
    zxc_dctx * const dctx;
};

/// Server-owned pools of idle contexts by block size and CPU; `take` also looks in other CPUs' pools.
template <typename Context>
class ContextPools
{
public:
    static std::unique_ptr<Context> take(size_t block_size)
    {
        auto & pools = poolsFor(block_size);
        if (pools.num_idle.load(std::memory_order_relaxed) == 0)
            return nullptr;

        ServerOwnedMemoryScope server_owned;
        const size_t current = currentPool();
        for (size_t i = 0; i < numPools(); ++i)
        {
            auto & pool = pools.pools[(current + i) % numPools()];
            std::lock_guard lock(pool.mutex);
            if (!pool.idle.empty())
            {
                auto context = std::move(pool.idle.back());
                pool.idle.pop_back();
                --pools.num_idle;
                return context;
            }
        }
        return nullptr;
    }

    static void put(std::unique_ptr<Context> context)
    {
        auto & pools = poolsFor(context->block_size);
        ServerOwnedMemoryScope server_owned;
        auto & pool = pools.pools[currentPool()];
        std::lock_guard lock(pool.mutex);
        pool.idle.push_back(std::move(context));
        ++pools.num_idle;
    }

private:
    struct Pool
    {
        std::mutex mutex;
        std::vector<std::unique_ptr<Context>> idle; /// STYLE_CHECK_ALLOW_STD_CONTAINERS: process-wide, not charged to a query
    };

    struct PoolsOfBlockSize
    {
        std::atomic<size_t> num_idle = 0;
        std::unique_ptr<Pool[]> pools;
    };

    static size_t numPools()
    {
        static const size_t num_pools = PerCPU::getNumPossibleCPUs();
        return num_pools;
    }

    static size_t currentPool() { return static_cast<size_t>(PerCPU::getCurrentCPU()) % numPools(); }

    static PoolsOfBlockSize & poolsFor(size_t block_size)
    {
        static PoolsOfBlockSize * all = []
        {
            ServerOwnedMemoryScope server_owned;
            auto * res = new PoolsOfBlockSize[num_block_sizes];
            for (size_t i = 0; i < num_block_sizes; ++i)
                res[i].pools = std::make_unique<Pool[]>(numPools());
            return res;
        }();
        return all[blockSizeIndex(block_size)];
    }
};

/// Moves the charge of a pooled context between the global tracker and the query.
void chargeQuery(Int64 bytes)
{
    std::ignore = bytes > 0 ? CurrentMemoryTracker::allocNoThrow(bytes) : CurrentMemoryTracker::free(-bytes);
    MemoryTrackerBlockerInThread blocker(VariableContext::Global);
    total_memory_tracker.adjustWithUntrackedMemory(-bytes);
}

/// At most two pooled contexts per usable core and direction.
bool tooManyContexts(size_t num_contexts)
{
    return num_contexts > 2 * getNumberOfCPUCoresToUse();
}

}

uint8_t CompressionCodecZXC::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::ZXC);
}

void CompressionCodecZXC::updateHash(SipHash & hash) const
{
    getCodecDescription()->updateTreeHash(hash, /*ignore_aliases=*/ true);
}

UInt32 CompressionCodecZXC::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    /// The bound assumes the smallest block size, so it holds for any block size chosen below.
    return static_cast<UInt32>(zxc_compress_bound(uncompressed_size));
}

UInt32 CompressionCodecZXC::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    static std::atomic<size_t> num_contexts = 0;
    /// Returning a context to its pool must not throw `MEMORY_LIMIT_EXCEEDED`.
    LockMemoryExceptionInThread lock(VariableContext::Global);

    const size_t block_size = blockSizeFor(source_size);
    auto context = ContextPools<CompressionContext>::take(block_size);
    if (context && !context->canCompressAt(level))
    {
        ServerOwnedMemoryScope server_owned;
        context.reset();
        --num_contexts;
    }

    if (context)
        chargeQuery(context->trackedSize());
    else
    {
        context = std::make_unique<CompressionContext>(block_size, level);
        ++num_contexts;
    }

    /// No zxc checksum: ClickHouse checksums compressed blocks itself.
    int rc = ZXC_OK;
    if (context->current_level != level)
    {
        rc = zxc_cctx_set_param(context->cctx, ZXC_C_LEVEL, level);
        if (rc == ZXC_OK)
            context->current_level = level;
    }

    int64_t compressed_size = rc;
    if (rc == ZXC_OK)
        compressed_size = zxc_compress_cctx(context->cctx, source, source_size, dest, zxc_compress_bound(source_size));

    /// Release the context before throwing.
    if (tooManyContexts(num_contexts) || context->workspace.bytes() > max_pooled_compression_workspace)
    {
        context.reset();
        --num_contexts;
    }
    else
    {
        chargeQuery(-context->trackedSize());
        ContextPools<CompressionContext>::put(std::move(context));
    }

    if (compressed_size < 0)
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress with ZXC codec at level {}: {}",
            level, zxc_error_name(static_cast<int>(compressed_size)));

    return static_cast<UInt32>(compressed_size);
}

UInt32 CompressionCodecZXC::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    static std::atomic<size_t> num_contexts = 0;
    /// Decompression contexts are server-owned, as for `ZSTD`.
    ServerOwnedMemoryScope server_owned;
    LockMemoryExceptionInThread lock(VariableContext::Global);

    /// A static context accepts only its own block size.
    zxc_frame_info_t info{};
    if (int rc = zxc_get_frame_info(source, source_size, &info, sizeof(info)); rc != ZXC_OK)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress ZXC-encoded data: {}", zxc_error_name(rc));
    if (!isValidBlockSize(info.block_size))
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress ZXC-encoded data: invalid block size {}", info.block_size);

    auto context = ContextPools<DecompressionContext>::take(info.block_size);
    if (!context)
    {
        context = std::make_unique<DecompressionContext>(info.block_size);
        ++num_contexts;
    }

    int64_t decompressed_size = zxc_decompress_dctx(context->dctx, source, source_size, dest, uncompressed_size);

    if (tooManyContexts(num_contexts))
    {
        context.reset();
        --num_contexts;
    }
    else
        ContextPools<DecompressionContext>::put(std::move(context));

    if (decompressed_size < 0)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress ZXC-encoded data: {}",
            zxc_error_name(static_cast<int>(decompressed_size)));
    if (static_cast<UInt32>(decompressed_size) != uncompressed_size)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress ZXC-encoded data: decompressed size {} does not match the expected size {}",
            decompressed_size, uncompressed_size);

    return static_cast<UInt32>(decompressed_size);
}

CompressionCodecZXC::CompressionCodecZXC(int level_)
    : level(level_)
{
}

ASTPtr CompressionCodecZXC::getCodecDescription() const
{
    return makeCodecDescription("ZXC", {make_intrusive<ASTLiteral>(static_cast<UInt64>(level))});
}

void registerCodecZXC(CompressionCodecFactory & factory)
{
    UInt8 method_code = static_cast<UInt8>(CompressionMethodByte::ZXC);
    factory.registerCompressionCodec("ZXC", method_code, [&](const ASTPtr & arguments) -> CompressionCodecPtr
    {
        int level = CompressionCodecZXC::ZXC_DEFAULT_LEVEL;
        if (arguments && !arguments->children.empty())
        {
            if (arguments->children.size() != 1)
                throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE, "ZXC codec must have 1 parameter, given {}",
                    arguments->children.size());

            const auto * literal = arguments->children[0]->as<ASTLiteral>();
            if (!literal)
                throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "ZXC codec argument must be integer");

            level = static_cast<int>(literal->value.safeGet<UInt64>());
            if (level < zxc_min_level() || level > zxc_max_level())
                throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER,
                    "ZXC codec level must be between {} and {}, given {}",
                    zxc_min_level(), zxc_max_level(), level);
        }
        return std::make_shared<CompressionCodecZXC>(level);
    });
}

}
