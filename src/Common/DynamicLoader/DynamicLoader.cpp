#include <Common/DynamicLoader/DynamicLoader.h>
#include <Common/DynamicLoader/ThreadLocalStorage.h>

#include <Common/Exception.h>

#include <cstring>
#include <deque>
#include <unordered_set>

#include <cstdlib>
#include <limits.h>
#include <unistd.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_DLOPEN;
    extern const int CANNOT_DLSYM;
}

namespace DynamicLinker
{

namespace
{
    std::string directoryName(const std::string & path)
    {
        auto slash = path.find_last_of('/');
        return slash == std::string::npos ? std::string(".") : path.substr(0, slash);
    }

    std::string baseName(const std::string & path)
    {
        auto slash = path.find_last_of('/');
        return slash == std::string::npos ? path : path.substr(slash + 1);
    }

    /// Resolve `path` to a canonical absolute path, or return an empty string if it does not exist.
    std::string canonicalPath(const std::string & path)
    {
        char resolved[PATH_MAX];
        if (::realpath(path.c_str(), resolved) == nullptr)
            return {};
        return std::string(resolved);
    }

    /// Split a colon-separated path list (as in LD_LIBRARY_PATH) into its directories.
    std::vector<std::string> splitPathList(const char * list)
    {
        std::vector<std::string> result;
        if (list == nullptr)
            return result;
        const char * begin = list;
        while (true)
        {
            const char * colon = std::strchr(begin, ':');
            if (colon == nullptr)
            {
                if (*begin)
                    result.emplace_back(begin);
                break;
            }
            if (colon != begin)
                result.emplace_back(begin, colon);
            begin = colon + 1;
        }
        return result;
    }

    /// Expand the "$ORIGIN" token (the directory of the loading object) inside a run path.
    std::string expandOrigin(const std::string & run_path, const std::string & object_directory)
    {
        const std::string token = "$ORIGIN";
        std::string result;
        size_t position = 0;
        while (true)
        {
            size_t found = run_path.find(token, position);
            if (found == std::string::npos)
            {
                result.append(run_path, position, std::string::npos);
                break;
            }
            result.append(run_path, position, found - position);
            result.append(object_directory);
            position = found + token.size();
        }
        return result;
    }
}


DynamicLoader::DynamicLoader(Options options_)
    : options(std::move(options_))
{
    /// Default system library directories for the host architecture.
#if defined(__x86_64__)
    default_search_paths = {"/lib/x86_64-linux-gnu", "/usr/lib/x86_64-linux-gnu", "/lib64", "/usr/lib64"};
#elif defined(__aarch64__)
    default_search_paths = {"/lib/aarch64-linux-gnu", "/usr/lib/aarch64-linux-gnu"};
#endif
    default_search_paths.insert(default_search_paths.end(), {"/lib", "/usr/lib", "/usr/local/lib"});

    extra_search_paths = options.search_paths;

    /// The thread-local storage helper must resolve to our own implementation, never the host's.
    provided_symbols["__tls_get_addr"] = threadLocalStorageAccessor();

    if (options.provide_host_memory_functions)
    {
        provided_symbols["memcpy"] = reinterpret_cast<void *>(&std::memcpy);
        provided_symbols["memmove"] = reinterpret_cast<void *>(&std::memmove);
        provided_symbols["memset"] = reinterpret_cast<void *>(&std::memset);
        provided_symbols["memcmp"] = reinterpret_cast<void *>(&std::memcmp);
    }
}

DynamicLoader::~DynamicLoader()
{
    /// Release every remaining object. Finalizers and unmapping happen in each destructor.
    libraries_by_shared_object_name.clear();
    libraries_by_real_path.clear();
}


void DynamicLoader::provideSymbol(const std::string & name, void * address)
{
    provided_symbols[name] = address;
}

void DynamicLoader::addSearchPath(const std::string & directory)
{
    extra_search_paths.insert(extra_search_paths.begin(), directory);
}


std::string DynamicLoader::findLibrary(const std::string & name, const std::vector<std::string> & run_paths) const
{
    /// A name containing a slash is a path, used as given.
    if (name.find('/') != std::string::npos)
    {
        std::string resolved = canonicalPath(name);
        if (!resolved.empty())
            return resolved;
        throw Exception(ErrorCodes::CANNOT_DLOPEN, "Cannot find shared library '{}'", name);
    }

    /// Search order: the object's run paths, then explicit paths, then LD_LIBRARY_PATH, then system directories.
    std::vector<std::string> directories = run_paths;
    directories.insert(directories.end(), extra_search_paths.begin(), extra_search_paths.end());
    std::vector<std::string> environment_paths = splitPathList(::getenv("LD_LIBRARY_PATH"));
    directories.insert(directories.end(), environment_paths.begin(), environment_paths.end());
    directories.insert(directories.end(), default_search_paths.begin(), default_search_paths.end());

    for (const std::string & directory : directories)
    {
        std::string candidate = directory + "/" + name;
        if (::access(candidate.c_str(), F_OK) == 0)
            return canonicalPath(candidate);
    }

    throw Exception(ErrorCodes::CANNOT_DLOPEN, "Cannot find shared library '{}' in any search path", name);
}


LoadedLibrary * DynamicLoader::loadObject(const std::string & path, const std::string & requested_name)
{
    /// Deduplicate by canonical path so a library reached by two different names is loaded once.
    if (auto it = libraries_by_real_path.find(path); it != libraries_by_real_path.end())
        return it->second.get();

    auto library = std::make_unique<LoadedLibrary>(path, requested_name);
    LoadedLibrary * pointer = library.get();
    pointer->mapAndParse();

    libraries_by_real_path.emplace(path, std::move(library));
    /// Also index by shared-object name so a DT_NEEDED reaching the same object dedups even via a different path.
    libraries_by_shared_object_name.emplace(pointer->sharedObjectName(), pointer);
    return pointer;
}


void DynamicLoader::buildDependencyClosure(LoadedLibrary * root)
{
    std::deque<LoadedLibrary *> queue{root};
    while (!queue.empty())
    {
        LoadedLibrary * library = queue.front();
        queue.pop_front();
        if (library->dependencies_built)
            continue;
        library->dependencies_built = true;

        std::string object_directory = directoryName(library->path());
        std::vector<std::string> run_paths;
        run_paths.reserve(library->runPaths().size());
        for (const std::string & run_path : library->runPaths())
            run_paths.push_back(expandOrigin(run_path, object_directory));

        for (const std::string & needed : library->neededLibraries())
        {
            /// A dependency already loaded by its shared-object name is reused without touching the filesystem.
            LoadedLibrary * dependency;
            if (auto it = libraries_by_shared_object_name.find(needed); it != libraries_by_shared_object_name.end())
                dependency = it->second;
            else
                dependency = loadObject(findLibrary(needed, run_paths), needed);

            library->dependencyList().push_back(dependency);
            queue.push_back(dependency);
        }
    }
}


ResolvedSymbol DynamicLoader::resolveSymbol(
    const std::vector<LoadedLibrary *> & scope, const char * name, const VersionRequirement & version) const
{
    /// The first definition in scope order wins, matching the ELF rule for a dlopen'd object's local scope.
    for (const LoadedLibrary * library : scope)
        if (ResolvedSymbol resolved = library->lookup(name, version))
            return resolved;

    /// Only then fall back to symbols the host explicitly provides - never to the host's own symbol table.
    if (auto it = provided_symbols.find(name); it != provided_symbols.end())
        return ResolvedSymbol{nullptr, nullptr, it->second, true};

    return {};
}


void DynamicLoader::runInitializersRecursive(LoadedLibrary * library, std::vector<LoadedLibrary *> & already_initialized)
{
    for (LoadedLibrary * existing : already_initialized)
        if (existing == library)
            return;
    already_initialized.push_back(library);

    /// Initialize dependencies first, so an object's initializers see a ready environment.
    for (LoadedLibrary * dependency : library->dependencyList())
        runInitializersRecursive(dependency, already_initialized);

    library->runInitializers();
}

void DynamicLoader::runInitializersInDependencyOrder(LoadedLibrary * root)
{
    std::vector<LoadedLibrary *> already_initialized;
    runInitializersRecursive(root, already_initialized);
}


LoadedLibrary * DynamicLoader::open(const std::string & path)
{
    /// Locate and map the requested object (dedup returns an already-open instance).
    std::string resolved_path;
    if (path.find('/') != std::string::npos)
    {
        resolved_path = canonicalPath(path);
        if (resolved_path.empty())
            throw Exception(ErrorCodes::CANNOT_DLOPEN, "Cannot find shared library '{}'", path);
    }
    else
    {
        resolved_path = findLibrary(path, {});
    }

    LoadedLibrary * root = loadObject(resolved_path, baseName(resolved_path));

    /// Load the whole dependency closure and compute the breadth-first resolution scope from `root`.
    buildDependencyClosure(root);

    std::vector<LoadedLibrary *> scope;
    {
        std::unordered_set<const LoadedLibrary *> seen;
        std::deque<LoadedLibrary *> queue{root};
        seen.insert(root);
        while (!queue.empty())
        {
            LoadedLibrary * library = queue.front();
            queue.pop_front();
            scope.push_back(library);
            for (LoadedLibrary * dependency : library->dependencyList())
                if (seen.insert(dependency).second)
                    queue.push_back(dependency);
        }
    }

    /// Relocate every not-yet-relocated object using the shared scope, then harden its RELRO region.
    auto resolver = [&](const char * name, const VersionRequirement & version)
    {
        return resolveSymbol(scope, name, version);
    };

    std::vector<LoadedLibrary *> newly_relocated;
    for (LoadedLibrary * library : scope)
    {
        if (!library->relocated)
        {
            library->relocated = true;
            library->applyRelocations(resolver);
            newly_relocated.push_back(library);
        }
    }
    for (LoadedLibrary * library : newly_relocated)
        library->protectReadOnlyRelocations();

    /// Run static initializers (dependencies first), then account for this open in every object's reference count.
    runInitializersInDependencyOrder(root);
    for (LoadedLibrary * library : scope)
        library->incrementReferenceCount();

    return root;
}


void * DynamicLoader::getSymbol(LoadedLibrary * library, const std::string & name)
{
    /// dlsym searches the object and its dependency closure.
    std::unordered_set<const LoadedLibrary *> seen;
    std::deque<LoadedLibrary *> queue{library};
    seen.insert(library);
    while (!queue.empty())
    {
        LoadedLibrary * current = queue.front();
        queue.pop_front();

        if (ResolvedSymbol resolved = current->lookup(name.c_str(), VersionRequirement{}))
            return resolved.address;

        for (LoadedLibrary * dependency : current->dependencyList())
            if (seen.insert(dependency).second)
                queue.push_back(dependency);
    }

    return nullptr;
}


void DynamicLoader::unloadRecursive(LoadedLibrary * library)
{
    libraries_by_shared_object_name.erase(library->sharedObjectName());
    /// Erasing the owning unique_ptr runs the destructor, which finalizes and unmaps.
    libraries_by_real_path.erase(library->path());
}


void DynamicLoader::close(LoadedLibrary * library)
{
    if (library == nullptr)
        return;

    /// Recompute the same scope the matching open() used, and drop one reference from each object.
    std::vector<LoadedLibrary *> scope;
    {
        std::unordered_set<const LoadedLibrary *> seen;
        std::deque<LoadedLibrary *> queue{library};
        seen.insert(library);
        while (!queue.empty())
        {
            LoadedLibrary * current = queue.front();
            queue.pop_front();
            scope.push_back(current);
            for (LoadedLibrary * dependency : current->dependencyList())
                if (seen.insert(dependency).second)
                    queue.push_back(dependency);
        }
    }

    if (library->referenceCount() == 0)
        throw Exception(ErrorCodes::CANNOT_DLSYM, "Closing a library '{}' that is not open", library->path());

    std::vector<LoadedLibrary *> to_unload;
    for (LoadedLibrary * current : scope)
        if (current->decrementReferenceCount() == 0)
            to_unload.push_back(current);

    /// Finalize dependents before dependencies. `scope` is breadth-first from the root, so its order works.
    for (LoadedLibrary * current : to_unload)
        current->runFinalizers();
    for (LoadedLibrary * current : to_unload)
        unloadRecursive(current);
}

}

}
