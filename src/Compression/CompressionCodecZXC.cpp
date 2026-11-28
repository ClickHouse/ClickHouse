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

#include <zxc.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_COMPRESS;
    extern const int CANNOT_DECOMPRESS;
    extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
    extern const int ILLEGAL_CODEC_PARAMETER;
    extern const int LOGICAL_ERROR;
}

namespace
{

/// zxc contexts are carved in workspaces allocated here (static context API) instead of per call,
/// and idle ones are pooled process-wide.

constexpr std::align_val_t workspace_alignment{64}; /// zxc requires a cache-line aligned workspace.

/// Largest zxc block size written; a larger input is split into several zxc blocks. A context thus never takes more memory
/// than with the one-shot API used before, which always took the default block size.
constexpr size_t max_block_size = ZXC_BLOCK_SIZE_DEFAULT;
static_assert(std::has_single_bit(max_block_size) && max_block_size >= ZXC_BLOCK_SIZE_MIN && max_block_size <= ZXC_BLOCK_SIZE_MAX);

/// No context with a larger workspace is pooled. This leaves out only levels 6 and 7 on large blocks.
constexpr size_t max_pooled_workspace = 2_MiB;

constexpr size_t num_pooled_block_sizes = std::countr_zero(max_block_size) - ZXC_BLOCK_SIZE_MIN_LOG2 + 1;

/// Smallest zxc block size holding the whole input, up to `max_block_size`. The decoder reads it from the frame header.
size_t blockSizeFor(UInt32 source_size)
{
    return std::clamp<size_t>(std::bit_ceil(static_cast<size_t>(source_size)), ZXC_BLOCK_SIZE_MIN, max_block_size);
}

bool isValidBlockSize(size_t block_size)
{
    return std::has_single_bit(block_size) && block_size >= ZXC_BLOCK_SIZE_MIN && block_size <= ZXC_BLOCK_SIZE_MAX;
}

/// Data written by other versions may use larger blocks; their contexts are not pooled.
bool isPooledBlockSize(size_t block_size)
{
    return isValidBlockSize(block_size) && block_size <= max_block_size;
}

size_t blockSizeIndex(size_t block_size)
{
    chassert(isPooledBlockSize(block_size));
    return static_cast<size_t>(std::countr_zero(block_size)) - ZXC_BLOCK_SIZE_MIN_LOG2;
}

/// zxc returns 0 for parameters it does not support.
size_t checkWorkspaceSize(size_t size, size_t block_size)
{
    if (size == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported parameters for a ZXC context with blocks of {} bytes", block_size);
    return size;
}

class Workspace
{
public:
    explicit Workspace(size_t size_) : size(size_), data(::operator new(size_, workspace_alignment)) {}
    ~Workspace() { ::operator delete(data, size, workspace_alignment); }

    Workspace(const Workspace &) = delete;
    Workspace & operator=(const Workspace &) = delete;

    [[nodiscard]] void * get() const { return data; }
    [[nodiscard]] size_t bytes() const { return size; }

    [[nodiscard]] Int64 trackedSize() const { return ::Memory::getActualAllocationSize(size, workspace_alignment); }

private:
    const size_t size;
    void * const data;
};

struct CompressionContext
{
    /// From `ZXC_LEVEL_DENSITY`, a level needs the optimal-parser scratch that lower levels do not carve,
    /// so the two tiers are pooled apart. Levels within a tier share the same workspace.
    static constexpr size_t num_pool_indices = num_pooled_block_sizes * 2;
    static size_t poolIndexFor(size_t block_size, int level) { return blockSizeIndex(block_size) * 2 + (level >= ZXC_LEVEL_DENSITY); }

    CompressionContext(size_t block_size_, int level_)
        : block_size(block_size_)
        , carved_level(level_)
        , current_level(level_)
        , workspace(checkWorkspaceSize(zxc_static_cctx_workspace_size(block_size_, level_), block_size_))
        , cctx(zxc_init_static_cctx(workspace.get(), workspace.bytes(), block_size_, level_))
    {
        if (!cctx)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Cannot create a ZXC compression context for blocks of {} bytes at level {}", block_size, carved_level);
    }

    [[nodiscard]] size_t poolIndex() const { return poolIndexFor(block_size, carved_level); }
    [[nodiscard]] Int64 trackedSize() const { return ::Memory::getActualAllocationSize(sizeof(*this)) + workspace.trackedSize(); }

    const size_t block_size;
    const int carved_level;
    int current_level;
    Workspace workspace;
    /// Points inside `workspace`.
    zxc_cctx * const cctx;
    /// Next idle context in the same pool.
    CompressionContext * next = nullptr;
};

struct DecompressionContext
{
    static constexpr size_t num_pool_indices = num_pooled_block_sizes;
    static size_t poolIndexFor(size_t block_size) { return blockSizeIndex(block_size); }

    explicit DecompressionContext(size_t block_size_)
        : block_size(block_size_)
        , workspace(checkWorkspaceSize(zxc_static_dctx_workspace_size(block_size_), block_size_))
        , dctx(zxc_init_static_dctx(workspace.get(), workspace.bytes(), block_size_))
    {
        if (!dctx)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot create a ZXC decompression context for blocks of {} bytes", block_size);
    }

    [[nodiscard]] size_t poolIndex() const { return poolIndexFor(block_size); }
    [[nodiscard]] Int64 trackedSize() const { return ::Memory::getActualAllocationSize(sizeof(*this)) + workspace.trackedSize(); }

    const size_t block_size;
    Workspace workspace;
    /// Points inside `workspace`.
    zxc_dctx * const dctx;
    /// Next idle context in the same pool.
    DecompressionContext * next = nullptr;
};

/// Server-owned pools of idle contexts: one per CPU for each pool index. Idle contexts take at most two maximal pooled
/// workspaces per usable core in all. Pools are intrusive lists, so that returning a context never allocates.
template <typename Context>
class ContextPools
{
public:
    /// An idle context from the pool of the current CPU, or else of another CPU (threads roam over CPUs), or nullptr.
    static std::unique_ptr<Context> take(size_t index)
    {
        auto & state = getState();
        auto & of_index = state.of_index[index];
        const size_t num_cpus = numCPUs();
        const size_t current = currentCPU();
        for (size_t i = 0; i < num_cpus && of_index.num_idle.load(std::memory_order_relaxed) > 0; ++i)
        {
            auto & pool = of_index.pools[(current + i) % num_cpus];
            Context * context = nullptr;
            {
                std::lock_guard lock(pool.mutex);
                context = pool.head;
                if (context)
                {
                    pool.head = context->next;
                    --of_index.num_idle;
                }
            }
            if (context)
            {
                context->next = nullptr;
                state.idle_bytes.fetch_sub(context->trackedSize(), std::memory_order_relaxed);
                return std::unique_ptr<Context>(context);
            }
        }
        return nullptr;
    }

    /// Moves `context` into the pool of the current CPU, unless it is too large or does not fit in the budget:
    /// then it stays with the caller and false is returned.
    static bool tryPut(std::unique_ptr<Context> & context)
    {
        if (!isPooledBlockSize(context->block_size) || context->workspace.bytes() > max_pooled_workspace)
            return false;

        auto & state = getState();
        const size_t index = context->poolIndex();
        auto & of_index = state.of_index[index];
        const Int64 size = context->trackedSize();
        while (!tryReserve(state, size))
        {
            /// The budget is held by contexts of other pool indices only: the workload has changed, so make room for this one.
            if (of_index.num_idle.load(std::memory_order_relaxed) > 0 || !evictOther(index))
                return false;
        }

        auto & pool = of_index.pools[currentCPU()];
        std::lock_guard lock(pool.mutex);
        context->next = pool.head;
        pool.head = context.release();
        ++of_index.num_idle;
        return true;
    }

private:
    struct Pool
    {
        std::mutex mutex;
        Context * head = nullptr;
    };

    struct PoolsOfIndex
    {
        std::atomic<size_t> num_idle = 0;
        std::unique_ptr<Pool[]> pools;
    };

    struct State
    {
        PoolsOfIndex of_index[Context::num_pool_indices];
        std::atomic<Int64> idle_bytes = 0;
    };

    static size_t numCPUs()
    {
        static const size_t num_cpus = PerCPU::getNumPossibleCPUs();
        return num_cpus;
    }

    static size_t currentCPU() { return static_cast<size_t>(PerCPU::getCurrentCPU()) % numCPUs(); }

    static State & getState()
    {
        static State * state = []
        {
            ServerOwnedMemoryScope server_owned;
            auto * res = new State;
            for (auto & of_index : res->of_index)
                of_index.pools = std::make_unique<Pool[]>(numCPUs());
            return res;
        }();
        return *state;
    }

    static bool tryReserve(State & state, Int64 size)
    {
        const Int64 budget = 2 * static_cast<Int64>(getNumberOfCPUCoresToUse()) * static_cast<Int64>(max_pooled_workspace);
        if (state.idle_bytes.fetch_add(size, std::memory_order_relaxed) + size <= budget)
            return true;
        state.idle_bytes.fetch_sub(size, std::memory_order_relaxed);
        return false;
    }

    /// Frees an idle context of another pool index; false if there is none.
    static bool evictOther(size_t index)
    {
        for (size_t i = 1; i < Context::num_pool_indices; ++i)
        {
            if (auto victim = take((index + i) % Context::num_pool_indices))
            {
                ServerOwnedMemoryScope server_owned;
                victim.reset();
                return true;
            }
        }
        return false;
    }
};

/// Moves the charge of a pooled context between the global tracker and the query.
void chargeQuery(Int64 bytes)
{
    std::ignore = bytes > 0 ? CurrentMemoryTracker::allocNoThrow(bytes) : CurrentMemoryTracker::free(-bytes);
    MemoryTrackerBlockerInThread blocker(VariableContext::Global);
    total_memory_tracker.adjustWithUntrackedMemory(-bytes);
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

UInt32 CompressionCodecZXC::getAdditionalSizeAtTheEndOfBuffer() const
{
    /// zxc decodes straight into `dest` only with `ZXC_DECOMPRESS_TAIL_PAD` bytes of room past the data for its wild copies,
    /// otherwise through its scratch buffer and a copy. This also covers its 32-byte over-reads past the compressed input.
    return static_cast<UInt32>(zxc_decompress_block_bound(0));
}

UInt32 CompressionCodecZXC::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    using Pools = ContextPools<CompressionContext>;
    /// Returning a context to its pool must not throw `MEMORY_LIMIT_EXCEEDED`.
    LockMemoryExceptionInThread lock(VariableContext::Global);

    const size_t block_size = blockSizeFor(source_size);
    auto context = Pools::take(CompressionContext::poolIndexFor(block_size, level));
    if (context)
        chargeQuery(context->trackedSize());
    else
        context = std::make_unique<CompressionContext>(block_size, level);

    /// No zxc checksum: ClickHouse checksums compressed blocks itself.
    int64_t compressed_size = ZXC_OK;
    if (context->current_level != level)
    {
        compressed_size = zxc_cctx_set_param(context->cctx, ZXC_C_LEVEL, level);
        if (compressed_size == ZXC_OK)
            context->current_level = level;
    }
    if (compressed_size == ZXC_OK)
        compressed_size = zxc_compress_cctx(context->cctx, source, source_size, dest, zxc_compress_bound(source_size));

    /// Release the context before throwing. A pooled context is no longer charged to the query.
    const Int64 context_size = context->trackedSize();
    if (Pools::tryPut(context))
        chargeQuery(-context_size);
    else
        context.reset();

    if (compressed_size < 0)
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress with ZXC codec at level {}: {}",
            level, zxc_error_name(static_cast<int>(compressed_size)));

    return static_cast<UInt32>(compressed_size);
}

UInt32 CompressionCodecZXC::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    using Pools = ContextPools<DecompressionContext>;
    LockMemoryExceptionInThread lock(VariableContext::Global);

    /// A static context accepts only its own block size.
    zxc_frame_info_t info{};
    if (int rc = zxc_get_frame_info(source, source_size, &info, sizeof(info)); rc != ZXC_OK)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress ZXC-encoded data: {}", zxc_error_name(rc));
    if (!isValidBlockSize(info.block_size))
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress ZXC-encoded data: invalid block size {}", info.block_size);

    std::unique_ptr<DecompressionContext> context;
    if (isPooledBlockSize(info.block_size))
        context = Pools::take(DecompressionContext::poolIndexFor(info.block_size));
    if (!context)
    {
        /// Decompression contexts are server-owned for their whole life.
        ServerOwnedMemoryScope server_owned;
        context = std::make_unique<DecompressionContext>(info.block_size);
    }

    /// Callers reserve `getAdditionalSizeAtTheEndOfBuffer` bytes past `dest`.
    const int64_t decompressed_size = zxc_decompress_dctx(context->dctx, source, source_size, dest,
        static_cast<size_t>(uncompressed_size) + getAdditionalSizeAtTheEndOfBuffer());

    if (!Pools::tryPut(context))
    {
        ServerOwnedMemoryScope server_owned;
        context.reset();
    }

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
