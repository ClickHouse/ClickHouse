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
      * every call at query time. The constructor calls it as well.
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

    /// The committed size: the length of the file, whose pages are all reserved. Never less than
    /// `size`, and greater only after a growth that committed its pages but could not map them.
    /// This is what the region costs, so memory accounting goes by this figure, not by `size`.
    size_t backingSize() const { return backing_size; }

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
};

using SharedMemoryRegionPtr = std::shared_ptr<SharedMemoryRegion>;

}
