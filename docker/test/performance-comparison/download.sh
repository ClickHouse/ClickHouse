#!/bin/bash
set -ex
set -o pipefail
trap "exit" INT TERM
trap 'kill $(jobs -pr) ||:' EXIT

mkdir db0 ||:
mkdir left ||:

left_pr=$1
left_sha=$2

right_pr=$3
right_sha=$4

datasets=${CHPC_DATASETS:-"hits1 hits10 hits100 values"}

declare -A dataset_paths
dataset_paths["hits10"]="https://s3.mds.yandex.net/clickhouse-private-datasets/hits_10m_single/partitions/hits_10m_single.tar"
dataset_paths["hits100"]="https://s3.mds.yandex.net/clickhouse-private-datasets/hits_100m_single/partitions/hits_100m_single.tar"
dataset_paths["hits1"]="https://clickhouse-datasets.s3.yandex.net/hits/partitions/hits_v1.tar"
dataset_paths["values"]="https://clickhouse-datasets.s3.yandex.net/values_with_expressions/partitions/test_values.tar"

function download
{
    # Historically there were various paths for the performance test package.
    # Test all of them.
    for path in "https://clickhouse-builds.s3.yandex.net/$left_pr/$left_sha/"{,clickhouse_build_check/}"performance/performance.tgz"
    do
        if curl --fail --head "$path"
        then
            left_path="$path"
        fi
    done

    # Might have the same version on left and right (for testing).
    if ! [ "$left_sha" = "$right_sha" ]
    then
        wget -nv -nd -c "$left_path" -O- | tar -C left --strip-components=1 -zxv  &
    else
        mkdir left ||:
        cp -a right/* left &
    fi

    for dataset_name in $datasets
    do
        dataset_path="${dataset_paths[$dataset_name]}"
        if [ "$dataset_path" = "" ]
        then
            >&2 echo "Unknown dataset '$dataset_name'"
            exit 1
        fi
        cd db0 && wget -nv -nd -c "$dataset_path" -O- | tar -xv &
    done

    mkdir ~/fg ||:
    (
        cd ~/fg
        wget -nv -nd -c "https://raw.githubusercontent.com/brendangregg/FlameGraph/master/flamegraph.pl"
        wget -nv -nd -c "https://raw.githubusercontent.com/brendangregg/FlameGraph/master/difffolded.pl"
        chmod +x ~/fg/difffolded.pl
        chmod +x ~/fg/flamegraph.pl
    ) &

    wait
}

download
