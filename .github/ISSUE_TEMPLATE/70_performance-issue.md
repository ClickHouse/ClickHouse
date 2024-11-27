---
name: Performance issue
about: Report something working slower than expected
title: ''
labels: performance
assignees: ''

---

(you don't have to strictly follow this form)

**Company or project name**
Put your company name or project description here

**Describe the situation**
What exactly works slower than expected?

**How to reproduce**
* Which ClickHouse server version to use
* Which interface to use, if matters
* Non-default settings, if any
* `CREATE TABLE` statements for all tables involved
* Sample data for all these tables, use [clickhouse-obfuscator](https://github.com/ClickHouse/ClickHouse/blob/master/programs/obfuscator/Obfuscator.cpp#L42-L80) if necessary
* Queries to run that lead to slow performance

**Expected performance**
What are your performance expectation, why do you think they are realistic? Has it been working faster in older ClickHouse releases? Is it working faster in some specific other system?

**Additional context**
Add any other context about the problem here.
