#!/usr/bin/env bash

# The script checks if all submodules defined in .gitmodules exist in contrib/,
# validates their URLs and names, and ensures there are no recursive submodules.

set -e

GIT_ROOT="."

cd "$GIT_ROOT"

# Remove keys for submodule.*.path parameters, the values are separated by \0
# and check if the directory exists
git config --file .gitmodules --null --get-regexp path | sed -z 's|.*\n||' | \
  while IFS= read -r -d '' submodule_path; do
    if ! test -d "$submodule_path"; then
        echo "Directory for submodule $submodule_path is not found"
        exit 1
    fi
  done 2>&1


# And check that the submodule is fine
git config --file .gitmodules --null --get-regexp path | sed -z 's|.*\n||' | \
  while IFS= read -r -d '' submodule_path; do
    git submodule status -q "$submodule_path"
  done 2>&1


# All submodules should be from https://github.com/
git config --file .gitmodules --get-regexp 'submodule\..+\.url' | \
while read -r line; do
    name=${line#submodule.}; name=${name%.url*}
    url=${line#* }
    [[ "$url" != 'https://github.com/'* ]] && echo "All submodules should be from https://github.com/, submodule '$name' has '$url'"
done

# All submodules should be of this form: [submodule "contrib/libxyz"] (for consistency, the submodule name does matter too much)
# - restrict the check to top-level .gitmodules file
git config --file .gitmodules --get-regexp 'submodule\..+\.path' | \
while read -r line; do
    name=${line#submodule.}; name=${name%.path*}
    path=${line#* }
    [ "$name" != "$path" ] && echo "Submodule name '$name' is not equal to its path '$path'"
done

# No recursive submodules allowed: check that no submodule contains its own .gitmodules with entries
git config --file .gitmodules --null --get-regexp path | sed -z 's|.*\n||' | \
  while IFS= read -r -d '' submodule_path; do
    if [ -f "$submodule_path/.gitmodules" ] && grep -q '\[submodule' "$submodule_path/.gitmodules"; then
        echo "Recursive submodules are not allowed: $submodule_path contains its own .gitmodules with submodule entries"
    fi
  done 2>&1
