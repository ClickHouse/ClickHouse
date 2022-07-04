MariaDB ColumnStore failed after 5 minutes of data loading:

```
ubuntu@ip-172-31-4-179:~$ time mysql --password="${PASSWORD}" --host 127.0.0.1 test -e "LOAD DATA LOCAL INFILE 'hits.tsv' INTO TABLE hits"
ERROR 1030 (HY000) at line 1: Got error -1 "Internal error < 0 (Not system error)" from storage engine ColumnStore
```

They don't have an issue tracker on GitHub, only JIRA.
JIRA requires login, but does not support SSO.
