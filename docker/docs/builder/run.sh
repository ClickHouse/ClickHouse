#!/bin/bash

set -ex

GIT_BRANCH=$(git branch --show-current)

if [ "$GIT_DOCS_BRANCH" ] && ! [ "$GIT_DOCS_BRANCH" == "$GIT_BRANCH" ]; then
  git fetch origin --depth=1 -- "$GIT_DOCS_BRANCH:$GIT_DOCS_BRANCH"
  git checkout "$GIT_DOCS_BRANCH"
else
  # Update docs repo
  git pull
fi

# The repo is usually mounted to /ClickHouse

for lang in en ru zh
do
  if [ -d "/ClickHouse/docs/${lang}" ]; then
    cp -rf "/ClickHouse/docs/${lang}" "/opt/clickhouse-docs/docs/"
  fi
done

# Generate pages with settings

./clickhouse -q "
WITH

'/ClickHouse/docs/en/operations/settings/settings.md' AS doc_file,
'/ClickHouse/src/Core/Settings.cpp' AS cpp_file,

settings_from_cpp AS
(
    SELECT extract(line, 'M\\(\\w+, (\\w+),') AS name
    FROM file(cpp_file, LineAsString)
    WHERE match(line, '^\\s*M\\(')
),

main_content AS
(
    SELECT format('## {} {}\\n\\nType: {}\\n\\nDefault value: {}\\n\\n{}\\n\\n', name, '{#'||name||'}', type, default, trim(BOTH '\\n' FROM description))
    FROM system.settings WHERE name IN settings_from_cpp
    ORDER BY name
),

(SELECT extract(raw_blob, '(^(?:[^#]|#[^#])+)##') FROM file(doc_file, RawBLOB)) AS prefix

SELECT prefix || (SELECT groupConcat(*) FROM main_content)
INTO OUTFILE '/opt/clickhouse-docs/docs/en/operations/settings/settings.md' TRUNCATE FORMAT LineAsString
"

./clickhouse -q "
WITH

'/ClickHouse/docs/en/operations/settings/settings-formats.md' AS doc_file,
'/ClickHouse/src/Core/FormatFactorySettingsDeclaration.h' AS cpp_file,

settings_from_cpp AS
(
    SELECT extract(line, 'M\\(\\w+, (\\w+),') AS name
    FROM file(cpp_file, LineAsString)
    WHERE match(line, '^\\s*M\\(')
),

main_content AS
(
    SELECT format('## {} {}\\n\\nType: {}\\n\\nDefault value: {}\\n\\n{}\\n\\n', name, '{#'||name||'}', type, default, trim(BOTH '\\n' FROM description))
    FROM system.settings WHERE name IN settings_from_cpp
    ORDER BY name
),

(SELECT extract(raw_blob, '(^(?:[^#]|#[^#])+)##') FROM file(doc_file, RawBLOB)) AS prefix

SELECT prefix || (SELECT groupConcat(*) FROM main_content)
INTO OUTFILE '/opt/clickhouse-docs/docs/en/operations/settings/settings-formats.md' TRUNCATE FORMAT LineAsString
"

# Force build error on wrong symlinks
sed -i '/onBrokenMarkdownLinks:/ s/ignore/error/g' docusaurus.config.js

if [[ $# -lt 1 ]] || [[ "$1" == "--"* ]]; then
  export CI=true
  exec yarn build "$@"
fi

exec "$@"
