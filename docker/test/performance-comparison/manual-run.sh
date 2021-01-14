#!/bin/bash
set -ex
set -o pipefail
trap "exit" INT TERM
trap 'kill $(jobs -pr) ||:' EXIT

stage=${stage:-}
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
repo_dir=${repo_dir:-$(readlink -f "$script_dir/../../..")}


function download
{
    rm -r left right db0 ||:
    mkdir left right db0 ||:

    "$script_dir/download.sh" ||: &
    cp -nvP "$repo_dir"/../build-gcc9-rel/programs/clickhouse* left &
    cp -nvP "$repo_dir"/../build-clang10-rel/programs/clickhouse* right &
    wait
}

function configure
{
    # Test files
    cp -nav "$repo_dir/tests/performance" right
    cp -nav "$repo_dir/tests/performance" left

    # Configs
    cp -nav "$script_dir/config" right
    cp -nav "$script_dir/config" left
    cp -nav "$repo_dir"/programs/server/config* right/config
    cp -nav "$repo_dir"/programs/server/user* right/config
    cp -nav "$repo_dir"/programs/server/config* left/config
    cp -nav "$repo_dir"/programs/server/user* left/config

    tree left
}

function run
{
    left/clickhouse-local --query "select * from system.build_options format PrettySpace" | sed 's/ *$//' | fold -w 80 -s > left-commit.txt
    right/clickhouse-local --query "select * from system.build_options format PrettySpace" | sed 's/ *$//' | fold -w 80 -s > right-commit.txt

    PATH=right:"$PATH" stage=configure "$script_dir/compare.sh" &> >(tee compare.log)
}

download
configure
run

rm output.7z
7z a output.7z ./*.{log,tsv,html,txt,rep,svg} {right,left}/{performance,db/preprocessed_configs}

