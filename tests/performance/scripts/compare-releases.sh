#!/bin/bash
set -ex
set -o pipefail
trap "exit" INT TERM
trap 'kill $(jobs -pr) ||:' EXIT

left_version=${1}
right_version=${2}

if [ "$left_version" == "" ] || [ "$right_version" == "" ]
then
    >&2 echo "Usage: $(basename "$0") left_version right_version"
    exit 1
fi

script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
repo_dir=${repo_dir:-$(readlink -f "$script_dir/../../..")}

function download_package() # (version, path)
{
    version="$1"
    path="$2"
    cd "$path"
    wget -nv -nd -nc "https://repo.clickhouse.com/deb/stable/main/clickhouse-common-static-dbg_${version}_amd64.deb" ||:
    wget -nv -nd -nc "https://repo.clickhouse.com/deb/stable/main/clickhouse-common-static_${version}_amd64.deb" ||:
    wget -nv -nd -nc "https://repo.clickhouse.com/deb/stable/main/clickhouse-test_${version}_all.deb" ||:
    mkdir tmp ||:
    for x in *.deb; do dpkg-deb -x "$x" tmp ; done
    mv tmp/usr/bin/clickhouse ./clickhouse
    mkdir .debug
    mv tmp/usr/lib/debug/usr/bin/clickhouse .debug/clickhouse
    mv tmp/usr/share/clickhouse-test/performance .
    ln -s clickhouse clickhouse-local
    ln -s clickhouse clickhouse-client
    ln -s clickhouse clickhouse-server
    rm -rf tmp
}

function download
{
    rm -r left right db0 ||:
    mkdir left right db0 ||:

    "$script_dir/download.sh" ||: &

    download_package "$left_version" left &
    download_package "$right_version" right &

    wait

    rm -rf {right,left}/tmp
}

function configure
{
    # Configs
    cp -av "$script_dir/config" right
    cp -av "$script_dir/config" left
    cp -av "$repo_dir"/programs/server/config* right/config
    cp -av "$repo_dir"/programs/server/user* right/config
    cp -av "$repo_dir"/programs/server/config* left/config
    cp -av "$repo_dir"/programs/server/user* left/config
}

function run
{
    left/clickhouse-local --query "select * from system.build_options format PrettySpace" | sed 's/ *$//' | fold -w 80 -s > left-commit.txt
    right/clickhouse-local --query "select * from system.build_options format PrettySpace" | sed 's/ *$//' | fold -w 80 -s > right-commit.txt

    PATH=right:"$PATH" \
        CHPC_TEST_PATH=right/performance \
        stage=configure \
        "$script_dir/compare.sh" &> >(tee compare.log)
}

download
configure
run

rm output.7z
7z a output.7z ./*.{log,tsv,html,txt,rep,svg} {right,left}/{performance,db/preprocessed_configs}
