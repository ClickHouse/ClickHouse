#!/bin/bash -e

(
    sed '/^const data = \[$/q' index.html

    FIRST=1
    ls -1 */results/*.json | while read file
    do
        [ "${FIRST}" = "0" ] && echo ','
        cat "${file}"
        FIRST=0
    done

    echo ']; // end of data'
    sed '0,/^\]; \/\/ end of data$/d' index.html

) > index.html.new

mv index.html index.html.bak
mv index.html.new index.html
