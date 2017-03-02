#!/bin/bash
TESTDIR="$(dirname "$0")"

CVEs="CVE-2002-0059 CVE-2004-0797 CVE-2005-1849 CVE-2005-2096"

for CVE in $CVEs; do
    fail=0
    for testcase in ${TESTDIR}/${CVE}/*.gz; do
	../minigzip -d < "$testcase"
	# we expect that a 1 error code is OK
	# for a vulnerable failure we'd expect 134 or similar
	if [ $? -ne 1 ]; then
	    fail=1
	fi
    done
    if [ $fail -eq 0 ]; then
	echo "		--- zlib not vulnerable to $CVE ---";
    else
	echo "          --- zlib VULNERABLE to $CVE ---"; exit 1;
    fi
done

