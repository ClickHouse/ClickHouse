#!/bin/bash
set -ex
set -o pipefail
trap "exit" INT TERM
trap 'kill $(jobs -pr) ||:' EXIT
#S3_URL=${S3_URL:="https://clickhouse-builds.s3.amazonaws.com"}
#BUILD_NAME=${BUILD_NAME:-package_release}
#export S3_URL BUILD_NAME

cd ./tmp_ci

mkdir db0 ||:
mkdir left ||:

#left_pr=$1
#left_sha=$2
#
## right_pr=$3 not used for now
#right_sha=$4

datasets=${CHPC_DATASETS-"hits1 hits10 hits100 values"}

declare -A dataset_paths
dataset_paths["hits10"]="https://clickhouse-private-datasets.s3.amazonaws.com/hits_10m_single/partitions/hits_10m_single.tar"
dataset_paths["hits100"]="https://clickhouse-private-datasets.s3.amazonaws.com/hits_100m_single/partitions/hits_100m_single.tar"
dataset_paths["hits1"]="https://clickhouse-datasets.s3.amazonaws.com/hits/partitions/hits_v1.tar"
dataset_paths["values"]="https://clickhouse-datasets.s3.amazonaws.com/values_with_expressions/partitions/test_values.tar"


function download
{
#    # Historically there were various paths for the performance test package.
#    # Test all of them.
#    declare -a urls_to_try=(
#        "$S3_URL/PRs/$left_pr/$left_sha/$BUILD_NAME/performance.tar.zst"
#    )
#
#    for path in "${urls_to_try[@]}"
#    do
#        if curl --fail --head "$path"
#        then
#            left_path="$path"
#        fi
#    done

#    # Might have the same version on left and right (for testing) -- in this case we just copy
#    # already downloaded 'right' to the 'left. There is the third case when we don't have to
#    # download anything, for example in some manual runs. In this case, SHAs are not set.
#    if ! [ "$left_sha" = "$right_sha" ]
#    then
#        wget -nv -nd -c "$left_path" -O- | tar -C left --no-same-owner --strip-components=1 --zstd --extract --verbose  &
#    elif [ "$right_sha" != "" ]
#    then
#        mkdir left ||:
#        cp -an right/* left &
#    fi

    cd db0
    for dataset_name in $datasets
    do
        dataset_path="${dataset_paths[$dataset_name]}"
        done_flag="${dataset_name}.done"

        # Check if the dataset path exists
        if [ "$dataset_path" = "" ]; then
            >&2 echo "Unknown dataset '$dataset_name'"
            exit 1
        fi
        if [ -f "$done_flag" ]; then
            echo "Dataset '$dataset_name' already downloaded and unpacked. Skipping."
            continue
        fi
        echo "Processing dataset '$dataset_name'..."
        if wget -nv -nd -c "$dataset_path" -O- | tar --extract --verbose; then
            touch "$done_flag"
            echo "Dataset '$dataset_name' completed."
        else
            >&2 echo "Failed to process dataset '$dataset_name'."
            exit 1
        fi
    done
    cd -

    echo "ATTACH DATABASE default ENGINE=Ordinary" > db0/metadata/default.sql
    echo "ATTACH DATABASE datasets ENGINE=Ordinary" > db0/metadata/datasets.sql
    ls db0/metadata
}

download
