#!/usr/bin/env bash

usage() {
  echo "Usage: $0 ENVIRONMENT" >&2
  echo "Valid values for ENVIRONMENT: staging, production" >&2
  exit 1
}

case "$1" in
  staging|production)
    ENVIRONMENT="$1" ;;
  --help)
    usage ;;
  *)
    echo "Invalid argument" >&2
    usage ;;
esac

cd "$(dirname "$0")" || exit 1
SOURCE_SCRIPT='init_runner.sh'

check_response() {
  # Are we even in the interactive shell?
  [ -t 1 ] || return 1
  local request
  request="$1"
  read -rp "$request (y/N): " response
  case "$response" in
    [Yy])
      return 0
      # Your code to continue goes here
      ;;
    *)
      return 1
      ;;
  esac
}

check_dirty() {
  if [ -n "$(git status --porcelain=v2 "$SOURCE_SCRIPT")" ]; then
    echo "The $SOURCE_SCRIPT has uncommited changes, won't deploy it" >&2
    exit 1
  fi
}
GIT_HASH=$(git log -1 --format=format:%H)

header() {
  cat << EOF
#!/usr/bin/env bash

echo 'The $ENVIRONMENT script is generated from $SOURCE_SCRIPT, commit $GIT_HASH'

EOF
}

body() {
  local first_line
  first_line=$(sed -n '/^# THE SCRIPT START$/{=;q}' "$SOURCE_SCRIPT")
  if [ -z "$first_line" ]; then
    echo "The pattern '# THE SCRIPT START' is not found in $SOURCE_SCRIPT" >&2
    exit 1
  fi
  tail "+$first_line" "$SOURCE_SCRIPT"
}

GENERATED_FILE="generated_${ENVIRONMENT}_${SOURCE_SCRIPT}"

{ header && body; } > "$GENERATED_FILE"

echo "The file $GENERATED_FILE is generated"

if check_response "Display the content of $GENERATED_FILE?"; then
  if [ -z "$PAGER" ]; then
    less "$GENERATED_FILE"
  else
    $PAGER "$GENERATED_FILE"
  fi
fi

check_dirty

S3_OBJECT=${S3_OBJECT:-s3://github-runners-data/cloud-init/${ENVIRONMENT}.sh}
if check_response "Deploy the generated script to $S3_OBJECT?"; then
  aws s3 mv "$GENERATED_FILE" "$S3_OBJECT"
fi
