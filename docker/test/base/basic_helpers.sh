#
# Basic helpers
#

# Usage: run_with_retry RETRIES ...
function run_with_retry()
{
    local retries="$1" && shift

    local retry=0
    until [ "$retry" -ge "$retries" ]; do
        if "$@"; then
            return
        else
            retry=$((retry + 1))
            sleep 5
        fi
    done

    echo "Command '$*' failed after $retries retries, exiting" >&2
    return 1
}

