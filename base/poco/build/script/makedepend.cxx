#! /bin/sh
#
# makedepend.cxx
#
# Create dependency file, Compaq C++ version
# Usage: makedepend.gcc source target dir1 dir2 dir3 dir4 flags...
#

source=$1
shift
target=$1
shift
dir1=$1
shift
dir2=$1
shift
dir3=$1
shift
dir4=$1
shift

cxx -M $@ $source | sed "s#\(.*\.o$\)#$dir1/\1 $dir2/\1 $dir3/\1 $dir4/\1#" >$target
