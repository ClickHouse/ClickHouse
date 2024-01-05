#!/bin/bash

set -x

# we mount tests folder from repo to /usr/share
ln -s /usr/share/clickhouse-test/ci/download_release_packages.py /usr/bin/download_release_packages
ln -s /usr/share/clickhouse-test/ci/get_previous_release_tag.py /usr/bin/get_previous_release_tag

echo "Get previous release tag"
previous_release_tag=$(dpkg --info package_folder/clickhouse-client*.deb | grep "Version: " | awk '{print $2}' | cut -f1 -d'+' | get_previous_release_tag)
echo $previous_release_tag

echo "Download clickhouse packages from the previous release"
mkdir previous_release_package_folder

echo $previous_release_tag | download_release_packages && echo -e "Download script exit code$OK" >> /test_output/test_results.tsv \
    || echo -e "Download script failed$FAIL" >> /test_output/test_results.tsv

# Check if we downloaded previous release packages successfully
if ! [ "$(ls -A previous_release_package_folder/clickhouse-common-static_*.deb)" ]
then
    echo -e 'failure\tFailed to download previous release packages' > /test_output/check_status.tsv
    exit
fi

echo -e "Successfully downloaded previous release packages\tOK" >> /test_output/test_results.tsv

dpkg-deb -xv package_folder/clickhouse-common-static_*.deb package_folder/
dpkg-deb -xv previous_release_package_folder/clickhouse-common-static_*.deb previous_release_package_folder/

if ! [ "$(ls -A package_folder/usr/bin/clickhouse && ls -A previous_release_package_folder/usr/bin/clickhouse)" ]
then
    echo -e 'failure\tFailed extract clickhouse binary from deb packages' > /test_output/check_status.tsv
    exit
fi

package_folder/usr/bin/clickhouse local -q "select * from system.settings format Native" > new_settings.native
previous_release_package_folder/usr/bin/clickhouse local -q "select * from system.settings format Native" > old_settings.native

package_folder/usr/bin/clickhouse local -nmq "
CREATE TABLE old_settings AS file('old_settings.native');
CREATE TABLE new_settings AS file('new_settings.native');

SELECT
    name,
    new_settings.value AS new_value,
    old_settings.value AS old_value
FROM new_settings
LEFT JOIN old_settings ON new_settings.name = old_settings.name
WHERE (new_settings.value != old_settings.value) AND (name NOT IN (
    SELECT arrayJoin(tupleElement(changes, 'name'))
    FROM system.settings_changes
    WHERE version = extract(version(), '^(?:\\d+\\.\\d+)')
))
SETTINGS join_use_nulls = 1
INTO OUTFILE 'changed_settings.txt'
FORMAT Pretty;

SELECT name
FROM new_settings
WHERE (name NOT IN (
    SELECT name
    FROM old_settings
)) AND (name NOT IN (
    SELECT arrayJoin(tupleElement(changes, 'name'))
    FROM system.settings_changes
    WHERE version = '23.9'
))
INTO OUTFILE 'new_settings.txt'
FORMAT Pretty;
"

description="OK"
status="success"

if [ -s changed_settings.txt ]
then
    mv changed_settings.txt /test_output/
    echo -e "Changed settings are not reflected in settings changes history (see changed_settings.txt)\tFAIL" >> /test_output/test_results.tsv
    description="New or changed settings are not reflected in settings changes history"
    status="failure"
else
    echo -e "There are no changed settings or they are reflected in settings changes history\tOK" >> /test_output/test_results.tsv
fi

if [ -s new_settings.txt ]
then
    mv new_settings.txt /test_output/
    echo -e "New settings are not reflected in settings changes history (see new_settings.txt)\tFAIL" >> /test_output/test_results.tsv
    description="New of changed settings are not reflected in settings changes history"
    status="failure"
else
    echo -e "There are no new settings or they are reflected in settings changes history\tOK" >> /test_output/test_results.tsv
fi

echo -e "$status\t$description" > /test_output/check_status.tsv
