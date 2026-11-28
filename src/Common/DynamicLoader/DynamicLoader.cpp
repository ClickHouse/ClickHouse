#include <Common/DynamicLoader/DynamicLoader.h>
#include <Common/DynamicLoader/ThreadLocalStorage.h>

#include <Common/Exception.h>

#include <algorithm>
#include <cstring>
#include <deque>
#include <unordered_set>

#include <climits>
#include <cstdlib>
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

    /// The breadth-first list of an object and its whole dependency closure: the symbol resolution scope.
    std::vector<LoadedLibrary *> breadthFirstScope(LoadedLibrary * root)
    {
        std::vector<LoadedLibrary *> scope;
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
        return scope;
    }

    void appendInDependencyOrder(
        LoadedLibrary * library, std::unordered_set<const LoadedLibrary *> & visited, std::vector<LoadedLibrary *> & order)
    {
        if (!visited.insert(library).second)
            return;
        for (LoadedLibrary * dependency : library->dependencyList())
            appendInDependencyOrder(dependency, visited, order);
        order.push_back(library);
    }

    /// The objects reachable from `roots`, ordered so that every object comes after everything it depends on
    /// (a depth-first post-order). This is the order of initialization; its reverse is the order of teardown.
    std::vector<LoadedLibrary *> dependencyOrder(const std::vector<LoadedLibrary *> & roots)
    {
        std::vector<LoadedLibrary *> order;
        std::unordered_set<const LoadedLibrary *> visited;
        for (LoadedLibrary * root : roots)
            appendInDependencyOrder(root, visited, order);
        return order;
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

    /// Read once, here: the environment is not safe to read while other threads may be modifying it.
    environment_search_paths = splitPathList(::getenv("LD_LIBRARY_PATH")); // NOLINT(concurrency-mt-unsafe)

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
    /// Release every remaining object, the same way `close` does: dependents before their dependencies.
    std::vector<LoadedLibrary *> remaining;
    remaining.reserve(libraries_by_real_path.size());
    for (const auto & [_, library] : libraries_by_real_path)
        remaining.push_back(library.get());
    unload(remaining);
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
    if (name.contains('/'))
    {
        std::string resolved = canonicalPath(name);
        if (!resolved.empty())
            return resolved;
        throw Exception(ErrorCodes::CANNOT_DLOPEN, "Cannot find shared library '{}'", name);
    }

    /// Search order: the object's run paths, then explicit paths, then LD_LIBRARY_PATH, then system directories.
    std::vector<std::string> directories = run_paths;
    directories.insert(directories.end(), extra_search_paths.begin(), extra_search_paths.end());
    directories.insert(directories.end(), environment_search_paths.begin(), environment_search_paths.end());
    directories.insert(directories.end(), default_search_paths.begin(), default_search_paths.end());

    for (const std::string & directory : directories)
    {
        std::string candidate = directory + "/" + name;
        if (::access(candidate.c_str(), F_OK) == 0)
            return canonicalPath(candidate);
    }

    throw Exception(ErrorCodes::CANNOT_DLOPEN, "Cannot find shared library '{}' in any search path", name);
}


LoadedLibrary * DynamicLoader::loadObject(
    const std::string & path, const std::string & requested_name, std::vector<LoadedLibrary *> & newly_loaded)
{
    /// Deduplicate by canonical path so a library reached by two different names is loaded once.
    if (auto it = libraries_by_real_path.find(path); it != libraries_by_real_path.end())
        return it->second.get();

    auto library = std::make_unique<LoadedLibrary>(path, requested_name);
    LoadedLibrary * pointer = library.get();
    pointer->mapAndParse();

    /// Reserve first, so that an object which got into the map is certainly recorded for a rollback.
    newly_loaded.reserve(newly_loaded.size() + 1);
    libraries_by_real_path.emplace(path, std::move(library));
    newly_loaded.push_back(pointer);
    return pointer;
}


void DynamicLoader::buildDependencyClosure(std::vector<LoadedLibrary *> & newly_loaded)
{
    /// Only the objects mapped by this `open` need their DT_NEEDED followed: every other object went through
    /// a successful `open` already. `newly_loaded` grows while we iterate, which makes it a breadth-first queue.
    for (size_t i = 0; i < newly_loaded.size(); ++i)
    {
        LoadedLibrary * library = newly_loaded[i];

        std::string object_directory = directoryName(library->path());
        std::vector<std::string> run_paths;
        run_paths.reserve(library->runPaths().size());
        for (const std::string & run_path : library->runPaths())
            run_paths.push_back(expandOrigin(run_path, object_directory));

        for (const std::string & needed : library->neededLibraries())
        {
            /// Resolve the name through the search path of the referring object first, and deduplicate by the
            /// canonical path only then. A name alone does not identify an object: two plugins may each ship a
            /// different library under the same name, and each must get its own.
            LoadedLibrary * dependency = loadObject(findLibrary(needed, run_paths), needed, newly_loaded);
            library->dependencyList().push_back(dependency);
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


LoadedLibrary * DynamicLoader::loadAndLink(const std::string & path, std::vector<LoadedLibrary *> & newly_loaded)
{
    /// Locate and map the requested object (dedup returns an already-open instance).
    std::string resolved_path;
    if (path.contains('/'))
    {
        resolved_path = canonicalPath(path);
        if (resolved_path.empty())
            throw Exception(ErrorCodes::CANNOT_DLOPEN, "Cannot find shared library '{}'", path);
    }
    else
    {
        resolved_path = findLibrary(path, {});
    }

    LoadedLibrary * root = loadObject(resolved_path, baseName(resolved_path), newly_loaded);

    /// Load the whole dependency closure and compute the breadth-first resolution scope from `root`.
    buildDependencyClosure(newly_loaded);
    std::vector<LoadedLibrary *> scope = breadthFirstScope(root);

    auto resolver = [&](const char * name, const VersionRequirement & version)
    {
        return resolveSymbol(scope, name, version);
    };

    /// The new objects, each after everything it depends on.
    std::unordered_set<const LoadedLibrary *> is_new(newly_loaded.begin(), newly_loaded.end());
    std::vector<LoadedLibrary *> new_in_dependency_order;
    for (LoadedLibrary * library : dependencyOrder({root}))
        if (is_new.contains(library))
            new_in_dependency_order.push_back(library);

    /// Relocate every new object using the shared scope. This goes in two phases over all of them: an indirect
    /// function (IFUNC) resolver is run on behalf of whoever refers to the function, but it is code of the
    /// object that defines it, so it must not run before that object has its ordinary relocations applied -
    /// whichever of the two comes first in the scope.
    for (LoadedLibrary * library : new_in_dependency_order)
        library->applyRelocations(resolver);
    /// Dependencies first, so that a resolver calling into a dependency finds it completely relocated.
    for (LoadedLibrary * library : new_in_dependency_order)
        library->applyIndirectRelocations(resolver);

    /// Harden the RELRO regions, then run the static initializers so that an object's initializers see a ready
    /// environment.
    for (LoadedLibrary * library : new_in_dependency_order)
        library->protectReadOnlyRelocations();
    for (LoadedLibrary * library : new_in_dependency_order)
        library->runInitializers();

    /// Account for this open in the reference count of every object of the scope.
    for (LoadedLibrary * library : scope)
        library->incrementReferenceCount();

    return root;
}


LoadedLibrary * DynamicLoader::open(const std::string & path)
{
    /// Everything this call maps. If the call fails at any step (a missing dependency, an undefined symbol, an
    /// unsupported relocation, a throwing initializer), these objects are unloaded again. Otherwise a half-built
    /// object would stay cached, and a retry would take it for a fully linked one.
    std::vector<LoadedLibrary *> newly_loaded;
    try
    {
        return loadAndLink(path, newly_loaded);
    }
    catch (...)
    {
        unload(newly_loaded);
        throw;
    }
}


void DynamicLoader::unload(const std::vector<LoadedLibrary *> & libraries) noexcept
{
    std::unordered_set<const LoadedLibrary *> to_unload(libraries.begin(), libraries.end());

    /// Destroying an object runs its finalizers (if its initializers have completed) and unmaps it. Going in the
    /// reverse of the initialization order tears down dependents first, while their dependencies are still mapped.
    std::vector<LoadedLibrary *> order = dependencyOrder(libraries);
    std::reverse(order.begin(), order.end());
    for (LoadedLibrary * library : order)
        if (to_unload.contains(library))
            libraries_by_real_path.erase(library->path());
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
            return resolved.finalAddress();

        for (LoadedLibrary * dependency : current->dependencyList())
            if (seen.insert(dependency).second)
                queue.push_back(dependency);
    }

    return nullptr;
}


void DynamicLoader::close(LoadedLibrary * library)
{
    if (library == nullptr)
        return;

    if (library->referenceCount() == 0)
        throw Exception(ErrorCodes::CANNOT_DLSYM, "Closing a library '{}' that is not open", library->path());

    /// Recompute the same scope the matching `open` used, and drop one reference from each object.
    std::vector<LoadedLibrary *> to_unload;
    for (LoadedLibrary * current : breadthFirstScope(library))
        if (current->decrementReferenceCount() == 0)
            to_unload.push_back(current);

    unload(to_unload);
}

}

}
