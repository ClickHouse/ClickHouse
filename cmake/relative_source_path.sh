#!/usr/bin/env bash
#
# A compiler launcher for reproducible builds (see ENABLE_BUILD_PATH_MAPPING):
#
#     relative_source_path.sh [<launcher>...] <compiler> <arguments>... -c [<options>...] <source> [<arguments>...]
#
# It runs the command with the source file, the first argument after `-c` that is not an option, as in the commands of CMake
# (`-c <source>`, sometimes followed by options of the source file) and of the `cc` crate (`-c <assembler options> <source>`),
# given relative to the working directory, if both are in the source
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

args=("$@")
source_index=-1
after_c=0
for ((i = 0; i < ${#args[@]}; ++i))
do
    if [ "$after_c" -eq 0 ]
    then
        if [ "${args[i]}" = "-c" ]
        then
            after_c=1
        fi
    elif [[ "${args[i]}" != -* ]]
    then
        source_index=$i
        break
    fi
done

if [ "$source_index" -lt 0 ]
then
    exec "$@"
fi

source="${args[source_index]}"
relative=""
case "$source" in
    "$source_dir/"*) relative="${source#"$source_dir/"}" ;;
    "$physical_source_dir/"*) relative="${source#"$physical_source_dir/"}" ;;
esac
if [ -z "$relative" ]
then
    exec "$@"
fi

args[source_index]="$up$relative"
if [ -n "$up" ]
then
    args+=("-ffile-prefix-map=$up=")
fi
exec "${args[@]}"
