#!/bin/bash

WNHOME=/usr/share/wordnet/

check() {
    local word_list="$1"
    echo "./bin/wntest $WNHOME ${word_list}"
    time ./bin/wntest $WNHOME ${word_list} > ${word_list}.blast
    echo "for i in \`cat ${word_list}\`; do wn $i -over; done"
    time for i in `cat ${word_list}`; do wn $i -over; done > ${word_list}.wn

    echo "diff ${word_list}.wn ${word_list}.blast -b"
    colordiff -y ${word_list}.wn ${word_list}.blast -b
}

check "$1"