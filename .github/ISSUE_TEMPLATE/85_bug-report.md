---
name: Bug report
about: Wrong behavior (visible to users) in the official ClickHouse release.
title: ''
labels: 'potential bug'
assignees: ''

---

> You have to provide the following information whenever possible.

**Describe what's wrong**

> A clear and concise description of what works not as it is supposed to.

**Does it reproduce on recent release?**

[The list of releases](https://github.com/ClickHouse/ClickHouse/blob/master/utils/list-versions/version_date.tsv)

**Enable crash reporting**

> If possible, change "enabled" to true in "send_crash_reports" section in `config.xml`:

```
<send_crash_reports>
        <!-- Changing <enabled> to true allows sending crash reports to -->
        <!-- the ClickHouse core developers team via Sentry https://sentry.io -->
        <enabled>false</enabled>
```

**How to reproduce**

* Which ClickHouse server version to use
* Which interface to use, if matters
* Non-default settings, if any
* `CREATE TABLE` statements for all tables involved
* Sample data for all these tables, use [clickhouse-obfuscator](https://github.com/ClickHouse/ClickHouse/blob/master/programs/obfuscator/Obfuscator.cpp#L42-L80) if necessary
* Queries to run that lead to unexpected result

**Expected behavior**

> A clear and concise description of what you expected to happen.

**Error message and/or stacktrace**

> If applicable, add screenshots to help explain your problem.

**Additional context**

> Add any other context about the problem here.
