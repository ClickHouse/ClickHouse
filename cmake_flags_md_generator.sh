#!/bin/bash/

output_file_name="cmake_flags_and_output.md"

regex='s/^((\s*#\s+.*\n?)*)\s*option\s*\(([A-Z_]+)\s*(\"((.|\n)*?)\")?\s*(.*)?\).*$/| \3 | \7 | \5 | \1 |\n/mg;t;d'

rm -fr ${output_file_name}
touch ${output_file_name}
cat cmake_files_header.md >> ${output_file_name}

process() {
    for i in "$1"/*.cmake "$1"/CMakeLists.txt;do
        if [ -d "$i" ];then
            process "$i"
        elif [ -f "$i" ]; then
            echo "Processing $i"

            cat $i | sed -E "${regex}" >> ${output_file_name}
        fi
    done
}

for base_folder in ./base ./cmake ./programs ./src; do
    process $base_folder
done
