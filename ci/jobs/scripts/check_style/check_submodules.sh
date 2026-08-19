#!/usr/bin/env bash

# The script checks if all submodules defined in $GIT_ROOT/.gitmodules exist in $GIT_ROOT/contrib

set -e

GIT_ROOT="."

cd "$GIT_ROOT"

# Remove keys for submodule.*.path parameters, the values are separated by \0
# and check if the directory exists
git config --file .gitmodules --null --get-regexp path | sed -z 's|.*\n||' | \
  xargs -P100 -0 --no-run-if-empty -I{} bash -c 'if ! test -d '"'{}'"'; then echo Directory for submodule {} is not found; exit 1; fi' 2>&1


# And check that the submodule is fine
git config --file .gitmodules --null --get-regexp path | sed -z 's|.*\n||' | \
  xargs -P100 -0 --no-run-if-empty -I{} git submodule status -q '{}' 2>&1


# All submodules should be from https://github.com/
git config --file "$ROOT_PATH/.gitmodules" --get-regexp 'submodule\..+\.url' | \
while read -r line; do
    name=${line#submodule.}; name=${name%.url*}
    url=${line#* }
    [[ "$url" != 'https://github.com/'* ]] && echo "All submodules should be from https://github.com/, submodule '$name' has '$url'"
done

# All submodules should be of this form: [submodule "contrib/libxyz"] (for consistency, the submodule name does matter too much)
# - restrict the check to top-level .gitmodules file
git config --file "$ROOT_PATH/.gitmodules" --get-regexp 'submodule\..+\.path' | \
while read -r line; do
    name=${line#submodule.}; name=${name%.path*}
    path=${line#* }
    [ "$name" != "$path" ] && echo "Submodule name '$name' is not equal to it's path '$path'"
done
