"""Use hermetic-llvm's targets for ClickHouse's pinned LLVM source tree."""

PREFIX = 'contrib/llvm-project'

# The toolchain supplies these headers in its own search directories. Importing
# another copy breaks libc++'s include_next chain before it reaches libc headers.
TOOLCHAIN_INCLUDE_DIRECTORIES = {
    PREFIX + '/libcxx/include',
    PREFIX + '/libcxx/include/c++/v1',
    PREFIX + '/libcxxabi/include',
}


def include_mappings(repository):
    """Canonical header namespaces provided by the module's CcInfo targets."""
    generated = 'bazel-out/k8-opt/bin/' + repository + '/'
    mappings = {
        generated + path: PREFIX + '/' + path
        for path in ['llvm/include', 'libcxx/include', 'libcxxabi/include',
                     'libunwind/include', 'compiler-rt/include', 'libc']
    }
    mappings.update({
        generated + 'libc/_virtual_includes/libc_headers': PREFIX + '/libc',
        generated + 'libcxx/libcxx_headers_include_search_directory': PREFIX + '/libcxx/include',
        generated + 'libcxxabi/libcxxabi_headers_include_search_directory': PREFIX + '/libcxxabi/include',
        generated + 'third-party/siphash/_virtual_includes/siphash': PREFIX + '/third-party/siphash/include',
    })
    for arch in ['x86_64', 'aarch64']:
        build = 'build_bazel_reference_' + arch + '/' + PREFIX + '/'
        mappings[build + 'libcxx/include/c++/v1'] = PREFIX + '/libcxx/include'
        mappings[build + 'llvm/include'] = PREFIX + '/llvm/include'
    return [{'from': source, 'to': destination} for source, destination in sorted(mappings.items())]


def generated_target(path):
    if path.startswith(PREFIX + '/'):
        return '@llvm-project//llvm'
    if path.startswith('contrib/compiler-rt-cmake/outline_atomic_helpers.dir/'):
        return '@llvm-project//compiler-rt:builtins'
    return None


def target(name):
    if name.startswith('LLVM'):
        component = name.removeprefix('LLVM')
        # The upstream Bazel overlay groups these CMake libraries together.
        component = {
            'ipo': 'IPO',
            'AsmPrinter': 'CodeGen',
            'GlobalISel': 'CodeGen',
            'SelectionDAG': 'CodeGen',
            'ObjCARCOpts': 'ObjCARC',
            'RuntimeDyld': 'ExecutionEngine',
            'Remarks': 'remark_linker',
            'ScalarOpts': 'Scalar',
            'X86Desc': 'X86UtilsAndDesc',
            'AArch64Desc': 'AArch64UtilsAndDesc',
            'AArch64Utils': 'AArch64UtilsAndDesc',
        }.get(component, component)
        return '@llvm-project//llvm:' + component
    return {
        'llvm-tblgen': '@llvm-project//llvm:llvm-tblgen',
        'llvm-min-tblgen': '@llvm-project//llvm:llvm-min-tblgen',
        'cxx': '@llvm-project//libcxx:libcxx',
        'cxxabi': '@llvm-project//libcxxabi:libcxxabi',
        'unwind': '@llvm-project//libunwind:libunwind',
        'clang_rt_builtins': '@llvm-project//compiler-rt:builtins',
        'clang_rt_xray': '@llvm-project//compiler-rt:xray',
        'libllvmlibc': '@llvm-project//libc:clickhouse_math',
        '_fuzzer': '@llvm-project//compiler-rt:fuzzer_main',
        '_fuzzer_no_main': '@llvm-project//compiler-rt:fuzzer',
    }.get(name)


def include_dependency(relative):
    if not relative.startswith(PREFIX + '/'):
        return None
    if relative == PREFIX + '/third-party/siphash/include':
        return '@llvm-project//third-party/siphash:siphash'
    project = relative.removeprefix(PREFIX + '/').split('/')[0]
    return {
        'llvm': '@llvm-project//llvm:Support',
        'libc': '@llvm-project//libc:libc_headers',
        'libcxx': '@llvm-project//libcxx:headers',
        'libcxxabi': '@llvm-project//libcxxabi:headers',
        'libunwind': '@llvm-project//libunwind:headers',
        'compiler-rt': '@llvm-project//compiler-rt:headers',
    }[project]
