namespace DB
{

/// This function tries to reallocate the code of the running program in a more efficient way.
void remapExecutable();

/// Find the address and size of the mapped memory region pointed by ptr.
std::pair<void *, size_t> getMappedArea(void * ptr);

}
