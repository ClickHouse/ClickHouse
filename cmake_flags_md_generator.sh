#!/bin/bash/

# https://regex101.com/r/R6iogw/7

output_file_name="cmake_flags_and_output.md"
ch_master_url="http:\/\/github.com\/clickhouse\/clickhouse\/blob\/master\/"

rm -fr ${output_file_name}
touch ${output_file_name}
cat cmake_files_header.md >> ${output_file_name}

process() {
    for i in "$1"/*.cmake "$1"/CMakeLists.txt;do
        echo "Processing $i"
        subd_name=${i//\//\\/}
        subd_name=${subd_name//\./\\\.}
        subd_name=${subd_name:2}
        regex='s/^((\s*#\s+.*\n?)*)\s*option\s*\(([A-Z_]+)\s*(\"((.|\n)*?)\")?\s*(.*)?\).*$/| [`\3`]('$ch_master_url${subd_name:2}') | `\7` | \5 | \1 |/mg;t;d'

        if [ -f $i ]; then
            cat $i | sed -E "$regex" >> ${output_file_name}
        fi
    done

    if [ "$2" = true ] ; then
        for d in "$1"/*;do
            if [ -d "$d" ];then
                process $d
            fi
        done
    fi
}

process ./ false

for base_folder in ./base ./cmake ./programs ./src; do
    process $base_folder true
done
