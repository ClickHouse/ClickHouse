#!/usr/bin/env bash
#
# A compiler launcher for reproducible builds (see ENABLE_BUILD_PATH_MAPPING):
#
#     relative_source_path.sh [<launcher>...] <compiler> <arguments>... <source>
#
# It runs the command with the source file, the last argument, as in the commands of CMake (`-c <source>`) and of the `cc`
# crate, given relative to the working directory, if both are in the source
# directory, the parent of the directory of this script.
#
# Clang writes the path of the source file, exactly as it is on the command line, into the module as `source_filename`, and
# `-ffile-prefix-map` does not apply to it. With LTO, the module goes into the object file, and the path into the hashes of the
# module and of its internal symbols, and so into the names of the internal symbols that ThinLTO promotes, such as
# `foo.llvm.1234`. CMake gives the compiler absolute paths, so the binary would depend on the directory of the checkout.
#
# The prefix of the relative path, such as `../`, is mapped to nothing with `-ffile-prefix-map`, so the debug info,
# `__FILE__` and `__builtin_FILE` have the same paths, relative to the source directory, as with an absolute path.
#
# CMake runs the commands in the build directory, which is usually inside the source directory (`build`, `ci/tmp/build`).
# The build scripts of Rust crates (the `cc` crate) run in the directory of the crate. For a build directory outside the
# source directory, the command runs unchanged.

source_dir="${BASH_SOURCE[0]%/cmake/relative_source_path.sh}"
physical_source_dir=$(cd "$source_dir" && pwd -P) || exit 1
physical_working_dir=$(pwd -P) || exit 1

case "$physical_working_dir/" in
    "$physical_source_dir/"*) ;;
    *) exec "$@" ;;
esac

# The path from the working directory to the source directory: `../` for every component of the working directory below it.
up=""
rest="${physical_working_dir#"$physical_source_dir"}"
IFS=/ read -r -a components <<< "$rest"
for component in "${components[@]}"
do
    if [ -n "$component" ]
    then
        up+="../"
    fi
done

if [ "$#" -eq 0 ]
then
    exit 1
fi
source="${!#}"
relative=""
case "$source" in
    "$source_dir/"*) relative="${source#"$source_dir/"}" ;;
    "$physical_source_dir/"*) relative="${source#"$physical_source_dir/"}" ;;
esac
if [ -z "$relative" ]
then
    exec "$@"
fi

args=("${@:1:$#-1}")
if [ -n "$up" ]
then
    args+=("-ffile-prefix-map=$up=")
fi
exec "${args[@]}" "$up$relative"
