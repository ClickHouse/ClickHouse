#!/bin/bash
set -ex
set -o pipefail
trap "exit" INT TERM
trap 'kill $(jobs -pr) ||:' EXIT

cd ./tmp_ci

mkdir db0 ||:
mkdir left ||:

datasets=${CHPC_DATASETS-"hits1 hits10 hits100 values tpch10 tpcds1"}

declare -A dataset_paths
dataset_paths["hits10"]="https://clickhouse-private-datasets.s3.amazonaws.com/hits_10m_single/partitions/hits_10m_single.tar"
dataset_paths["hits100"]="https://clickhouse-private-datasets.s3.amazonaws.com/hits_100m_single/partitions/hits_100m_single.tar"
dataset_paths["hits1"]="https://clickhouse-datasets.s3.amazonaws.com/hits/partitions/hits_v1.tar"
dataset_paths["values"]="https://clickhouse-datasets.s3.amazonaws.com/values_with_expressions/partitions/test_values.tar"
dataset_paths["tpch10"]="https://clickhouse-datasets.s3.amazonaws.com/h/10/tpch_sf10.tar"
dataset_paths["tpcds1"]="https://clickhouse-datasets.s3.amazonaws.com/ds/scale_1/tpcds.tar"


function download
{
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
