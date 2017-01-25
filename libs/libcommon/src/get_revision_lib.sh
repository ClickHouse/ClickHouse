# Filter non-release tags
function tag_filter {
	grep -E "^v1\.1\.[0-9]{5}-testing$"
}

# Get last revision number
function get_revision {
	BASEDIR=`dirname "$0"`
	CURDIR=`pwd`
	cd ${BASEDIR}
	git rev-parse --git-dir >/dev/null 2>/dev/null || cd ${CURDIR}
	git tag | tag_filter | tail -1 | sed 's/^v1\.1\.\(.*\)-testing$/\1/'
	cd ${CURDIR}
}
