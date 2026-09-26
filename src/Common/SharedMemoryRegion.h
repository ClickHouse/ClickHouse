#pragma once

#include <cstddef>
#include <memory>
#include <string>

namespace DB
{

/** A region of memory shared between the server and the process of an executable UDF.
  *
  * Backed by a `memfd` with `F_SEAL_SHRINK | F_SEAL_SEAL`. That choice is what makes everything
  * else about this class simple, and it is worth stating what it buys and what it costs.
  *
  * The command holds the region open for writing - it has to, its output goes there - and so it
  * could shrink the file. A server that had the region mapped would then take a `SIGBUS` on the
  * pages the file no longer backs, and that signal cannot be caught: it takes the whole server
  * down. No check can close that, because the command can shrink the file in the instant between
  * the check and the access. A sealed `memfd` closes it by construction: shrinking is refused by
  * the kernel, and `F_SEAL_SEAL` keeps the command - which has a writable descriptor - from adding
  * seals of its own, such as `F_SEAL_GROW` against the server's growth or `F_SEAL_WRITE` against
  * its writes. So the server serializes its input straight into the mapping and parses the output
  * where it lies, without a copy in either direction and without ever checking whether the file is
  * still whole. There is nothing to check.
  *
  * What the seal does not do is worth stating just as plainly, because it draws the line of what
  * this class promises. It stops the file getting shorter; it does not stop the command from
  * extending it (only the server's growth could be allowed, and seals do not tell the two apart -
  * see `refreshBackingSize`), and it does not stop the command from freeing pages inside it with
  * `fallocate(FALLOC_FL_PUNCH_HOLE)` or `madvise(MADV_REMOVE)`, which the kernel refuses only
  * under `F_SEAL_WRITE` - a seal the command cannot live with, its output goes into the file.
  * Neither is a `SIGBUS`: a page that was punched out is still inside the file, and the server's
  * next access to it allocates it afresh, like any other page of memory - which can fail the way
  * any allocation can (the OOM killer, under a cgroup limit; the mount behind a `memfd` has no
  * size limit of its own), but not the way an access past the end of a file does. What a punched
  * hole takes away is the reservation: the pages were committed up front so that the transport
  * would not allocate on the hot path, and after a hole it does, for that region. There is no
  * putting that back that would mean anything: the command can punch again the instant after,
  * and on `shmem` no cheap check even tells a hole from a reserved page (a reserved page is not
  * up to date until it is touched, so `mincore` and `SEEK_HOLE` report it as missing, and
  * `st_blocks` counts pages the command can park past the end of the file); committing the
  * whole file again on every call costs milliseconds per region, about what the transport saves.
  *
  * So the contract is this. The command is the server's own code - configured by the
  * administrator, run as the server's user, able to signal the server - and is trusted like it.
  * Against a command that is merely wrong (the classic one opens the region with `O_TRUNC`), the
  * seal is a kernel-enforced guarantee: the server cannot be crashed through the region. Against
  * a command that means harm, the region's cost is bounded - the consumer never charges, commits
  * or maps it beyond what it checked against its cap - and nothing else is promised: such a
  * command can slow its own function down, answer with zeros, or, for that matter, kill the
  * server outright, none of which is the transport's to prevent.
  *
  * A `memfd` has no name in any filesystem, which is the cost, and it is paid once: the command
  * cannot open the region by a path the server made up. Instead the descriptor is inherited by the
  * command's process at `exec` (see `ShellCommand::Config::inherited_fds`), and the request names it
  * as `/proc/self/fd/N`. That keeps the command's side of the protocol exactly what it was - open a
  * path, `mmap` it - and it means the region has to exist before the process is started.
  *
  * It also means the region can only grow. `grow` extends the file with `posix_fallocate` (which
  * both lengthens it and commits its pages, and undoes itself on failure) and replaces the server's
  * mapping; the command, which maps the whole file on every request, sees the new size. There is
  * no `shrink`: the seal forbids it, and a pooled region therefore stays as large as the largest
  * chunk it ever held for as long as its worker lives. `shared_memory_max_size` is what a worker
  * may hold, not what it may temporarily reach.
  *
  * Nothing here survives the server: an unnamed file dies with its last descriptor, so a server
  * that is killed leaves no region behind and there is nothing to reclaim.
  *
  * This class only owns the descriptor and the mapping; it does no memory accounting, because a
  * region can outlive a single query (it is reused across `executable_pool` borrows). The consumer
  * charges the query memory tracker per borrow.
  */
class SharedMemoryRegion
{
public:
    /** Throws `NOT_IMPLEMENTED` if this platform cannot back a shared-memory region.
      *
      * The region relies on `memfd_create` with sealing, so it is available on Linux only. Callers
      * that let a user enable the feature should call this at configuration time, so that an
      * unsupported platform is reported once, where the setting is accepted, instead of failing
      * every call at query time. The constructor does not repeat it: a transient failure of the
      * real creation (out of descriptors, out of memory) must report as what it is, not as the
      * transport being unavailable.
      */
    static void checkSupported();

    /// Creates a sealed region of `size` bytes, with its pages committed, and maps it.
    explicit SharedMemoryRegion(size_t size);

    ~SharedMemoryRegion();

    SharedMemoryRegion(const SharedMemoryRegion &) = delete;
    SharedMemoryRegion & operator=(const SharedMemoryRegion &) = delete;
    SharedMemoryRegion(SharedMemoryRegion &&) = delete;
    SharedMemoryRegion & operator=(SharedMemoryRegion &&) = delete;

    /** Grows the region to `new_size` bytes (must be strictly greater than the current size).
      *
      * The pages are committed first and the mapping is replaced only on success. A failure
      * before the pages are committed leaves the region exactly as it was. A failure after -
      * the replacement `mmap` failing - leaves the mapping, and so `size` and `data`, as they
      * were, but the file has already grown and cannot be shrunk back: `backingSize` then
      * reports the larger figure, which is what the region actually costs. `data` may return a
      * different pointer after a successful growth; callers must re-read it. The bytes written
      * so far survive either way.
      */
    void grow(size_t new_size);

    char * data() { return region_data; }
    const char * data() const { return region_data; }

    /// The mapped size: what `data` covers and what the transport may use.
    size_t size() const { return region_size; }

    /// The length of the file as last seen: never less than `size`, and greater after a growth
    /// that committed its pages but could not map them - or after the command extended the file
    /// (see `refreshBackingSize`). This is what the region costs, so memory accounting goes by this
    /// figure, not by `size`.
    size_t backingSize() const { return backing_size; }

    /// The footprint as last seen (see `refreshFootprint`): never less than `backingSize`, and
    /// never less than a page - a file holds whole pages, whatever its length. This is what the
    /// region costs, so memory accounting goes by this figure and by its changes.
    size_t footprint() const { return footprint_size; }

    /// The length up to which the server itself has committed the file's pages: at creation and
    /// at every growth of its own (`posix_fallocate` over the whole length). Never raised by what
    /// the command did to the file - it can extend the file without committing a page
    /// (`ftruncate`), and a length only observed says nothing about the pages under it. So this
    /// is the one baseline from which the cost of the server's next `posix_fallocate` can be
    /// bounded: the pages between here and the new length, at most, whatever the command did
    /// past here.
    size_t reservedSize() const { return reserved_size; }

    /// What committing the file's pages up to `length` - a growth to it, or mapping it whole -
    /// could add to the footprint, in whole pages. The footprint says how many pages the file
    /// holds, not where: the pages between what the server itself committed (`reservedSize`) and
    /// `length` may all be missing (a command can extend the file without committing a page),
    /// while pages the command committed past the end are in the footprint too, and the fill adds
    /// to them rather than uses them. How many of those there are at least is what the file holds
    /// beyond what the server committed - as if none of the server's own pages had been freed by
    /// the command - so the footprint after the fill is at least `length` plus those, and that
    /// is the figure. Pages the command freed and replaced with pages past the end hide from this
    /// (the count is the same), and by at most what the server committed - which the region was
    /// charged for; the next re-read after the fill finds them.
    size_t fillCostUpTo(size_t length) const
    {
        const size_t reserved = roundUpToPages(reserved_size);
        const size_t target = roundUpToPages(length);
        if (target <= reserved)
            return 0;
        const size_t past_the_end_at_least = committed_size > reserved ? committed_size - reserved : 0;
        const size_t footprint_after = target + past_the_end_at_least;
        return footprint_after > footprint_size ? footprint_after - footprint_size : 0;
    }

    /** Re-reads the length of the file and returns it, updating `backingSize`.
      *
      * The seals stop the command from shrinking the file; nothing stops it from extending it,
      * since only the server's own growth can be allowed and seals do not tell the two apart. A
      * command has no reason to (a result that does not fit is asked for through the protocol),
      * but one that does holds pages the server knows nothing about. So the size is read back
      * from the file wherever the region's charge changes hands - when a borrow starts and when
      * the worker goes back into the pool - and the larger figure is charged from then on. Between
      * those points the cached figure is used; a command extending the file mid-borrow is charged
      * at the next hand-over, not never.
      */
    size_t refreshBackingSize();

    /** Re-reads the file and returns what it costs: the larger of its length and the pages it has
      * committed (`st_blocks`), updating `backingSize` on the way.
      *
      * The two differ in both directions, and the command controls both. A file it extended is
      * longer than it is committed - a sparse tail. And a file can have more pages committed than
      * its length says: `fallocate(FALLOC_FL_KEEP_SIZE)` past the end of the file allocates pages
      * without moving the end, which a length-only figure never sees. Those pages are as real as
      * any others and live as long as the region does, so a cap or a charge that went by the
      * length alone could be walked around with one call. A page the command committed cannot
      * hide from `st_blocks`, whichever side of the end of the file it is on.
      */
    size_t refreshFootprint();

    /// Rounds a size up to whole pages: what a file of that length actually holds. The unit in
    /// which footprints are compared with caps and with each other, so that a region of 16 bytes
    /// is not over a cap of 16 bytes for holding the page it cannot help holding. The "page" is
    /// the unit the kernel backs a `memfd` in: the base page, or the transparent huge page where
    /// `shmem` is backed with those regardless of size (`shmem_enabled` `always`/`force`).
    static size_t roundUpToPages(size_t size);

    /// What the region would cost once mapped whole, as last read: the footprint plus the fill up
    /// to the length of the file (`fillCostUpTo`). The figure a cap is compared with, and the one
    /// to report when a region is over it - its length and its footprint can both be within the
    /// cap while this is not.
    size_t costOnceMappedWhole() const { return footprint_size + fillCostUpTo(backing_size); }

    /// Whether the region, as last read (`refreshFootprint`), is over a cap of `max_size` bytes.
    /// Three comparisons. The length of the file in bytes against the cap in bytes: the length is
    /// exact, and the command's to change, so it is held to the exact figure - a 24-byte file
    /// stretched to a page is a file stretched past a cap of 24 bytes, whatever the page count
    /// says. The footprint against the cap rounded up to pages, because a file holds whole pages:
    /// a 24-byte region holds one, and is not over its cap for it. And what the footprint would be
    /// once the server has committed the file's pages up to its length (`fillCostUpTo`), which
    /// mapping it whole entails: a file the command stretched to the cap without committing a page
    /// and then filled with as many pages past its end holds a cap's worth of pages and would hold
    /// two - and the server must not be the one to commit the second.
    bool isOverTheCap(size_t max_size) const
    {
        return backing_size > max_size || costOnceMappedWhole() > roundUpToPages(max_size);
    }

    /// The descriptor, for handing to the command's process at `exec`. Close-on-exec in this
    /// process; the hand-over `dup2`s it into the child, which clears the flag on the copy.
    int fd() const { return region_fd; }

    /// The path under which a process that inherited the descriptor as `child_fd` can open it.
    static std::string pathForChildFd(int child_fd);

private:
    int region_fd = -1;
    char * region_data = nullptr;
    size_t region_size = 0;
    size_t backing_size = 0;
    size_t reserved_size = 0;
    /// The pages the file holds (`st_blocks`), as last read. Can be less than what the server
    /// itself committed: the command can free pages under the length it reserved
    /// (`FALLOC_FL_PUNCH_HOLE`), and if it commits as many past the end instead, the count is the
    /// same and `fillCostUpTo` takes them for the server's - see there.
    size_t committed_size = 0;
    size_t footprint_size = 0;
};

using SharedMemoryRegionPtr = std::shared_ptr<SharedMemoryRegion>;

}
