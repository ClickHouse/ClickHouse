#! /bin/sh
#
# A script for running the POCO testsuites.
#
# usage: runtests [component [test] ]
#
# If the environment variable EXCLUDE_TESTS is set, containing
# a space-separated list of project names (as found in the
# components file), these tests will be skipped.
#
# Cygwin specific setup.
# ----------------------
# On Cygwin, Unix IPC are provided by a separate process daemon
# named cygserver, which should be started once before running any
# test from Foundation.
# 1/ Open a separate Cygwin terminal with Administrator privilege
# 2/ run the command: cygserver-configure
# 3/ Start the cygserver: nohup /usr/sbin/cygserver &
# 4/ close the separate terminal
# 5/ run the Foundation tests: build/script/runtests.sh Foundation
#

if [ "$POCO_BASE" = "" ] ; then
	POCO_BASE=`pwd`
fi

if [ "$POCO_BUILD" = "" ] ; then
	POCO_BUILD=$POCO_BASE
fi

TESTRUNNER=./testrunner

if [ "$1" = "" ] ; then
   components=`cat $POCO_BASE/components`
else
   components=$1
fi

if [ "$2" = "" ] ; then
    TESTRUNNERARGS=-all
else
    TESTRUNNERARGS=$2
fi

if [ "$OSARCH" = "" ] ; then
	OSARCH=`uname -m | tr ' /' _-`
fi

if [ "$OSNAME" = "" ] ; then
	OSNAME=`uname`
        case $OSNAME in
        CYGWIN*)
                OSNAME=CYGWIN
                TESTRUNNER=$TESTRUNNER.exe
                PATH=$POCO_BUILD/lib/$OSNAME/$OSARCH:$PATH
                ;;
        MINGW*)
                OSNAME=MinGW ;;
        esac
fi

BINDIR="bin/$OSNAME/$OSARCH/"

runs=0
failures=0
failedTests=""
status=0

for comp in $components ;
do
	excluded=0
	for excl in $EXCLUDE_TESTS ;
	do
		if [ "$excl" = "$comp" ] ; then
			excluded=1
		fi
	done
	if [ $excluded -eq 0 ] ; then
		if [ -d "$POCO_BUILD/$comp/testsuite/$BINDIR" ] ; then
			if [ -x "$POCO_BUILD/$comp/testsuite/$BINDIR/$TESTRUNNER" ] ; then
				echo ""
				echo ""
				echo "****************************************"
				echo "*** $OSNAME $OSARCH $comp"
				echo "****************************************"
				echo ""

				runs=`expr $runs + 1`
				sh -c "cd $POCO_BUILD/$comp/testsuite/$BINDIR && LD_LIBRARY_PATH=.:$LD_LIBRARY_PATH $TESTRUNNER $TESTRUNNERARGS"
				if [ $? -ne 0 ] ; then
					failures=`expr $failures + 1`
					failedTests="$failedTests $comp"
					status=1
				fi
			fi
		fi
	fi
done

echo ""
echo ""
echo "$runs runs, $failures failed."
echo ""
for test in $failedTests ;
do
	echo "Failed: $test"
done
echo ""

exit $status
