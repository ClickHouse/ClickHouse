---
name: Backward compatibility issue
about: Report the case when the behaviour of a new version can break existing use cases
title: ''
labels: backward compatibility
assignees: ''

---

(you don't have to strictly follow this form)

**Company or project name**
Put your company name or project description here

**Describe the issue**
A clear and concise description of what works not as it is supposed to.

**How to reproduce**
* Which ClickHouse server versions are incompatible
* Which interface to use, if matters
* Non-default settings, if any
* `CREATE TABLE` statements for all tables involved
* Sample data for all these tables, use [clickhouse-obfuscator](https://github.com/ClickHouse/ClickHouse/blob/master/programs/obfuscator/Obfuscator.cpp#L42-L80) if necessary
* Queries to run that lead to unexpected result

**Error message and/or stacktrace**
If applicable, add screenshots to help explain your problem.

**Additional context**
Add any other context about the problem here.
