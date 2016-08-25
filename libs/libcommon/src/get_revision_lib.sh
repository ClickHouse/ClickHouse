# Filter non-release tags
function tag_filter {
	grep -E "^v1\.1\.[0-9]{5}-testing$"
}

# Get last revision number
function get_revision {
	git fetch --tags
	git tag | tag_filter | tail -1 | sed 's/^v1\.1\.\(.*\)-testing$/\1/'
}
