# Get last revision number
function get_revision {
	BASEDIR=`dirname "$0"`
	grep "set(VERSION_REVISION" ${BASEDIR}/libs/libcommon/cmake/version.cmake | sed 's/^.*VERSION_REVISION \(.*\))$/\1/'
}
