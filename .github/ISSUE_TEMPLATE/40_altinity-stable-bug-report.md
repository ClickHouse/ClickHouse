---
name: Altinity Stable Bug report
about: Report something broken in an Altinity Stable Build
title: ''
labels: stable
assignees: ''

---

✅  *I checked [the Altinity Stable Builds lifecycle table](https://docs.altinity.com/altinitystablebuilds/#altinity-stable-builds-life-cycle-table), and the Altinity Stable Build version I'm using is still supported.*

## Type of problem
Choose one of the following items, then delete the others: 

**Bug report** - something's broken

**Incomplete implementation** - something's not quite right

**Performance issue** - something works, just not as quickly as it should

**Backwards compatibility issue** - something used to work, but now it doesn't

**Unexpected behavior** - something surprising happened, but it wasn't the good kind of surprise

**Installation issue** - something doesn't install the way it should

**Usability issue** - something works, but it could be a lot easier

**Documentation issue** - something in the docs is wrong, incomplete, or confusing

## Describe the situation
A clear, concise description of what's happening. Can you reproduce it in a ClickHouse Official build of the same version?

## How to reproduce the behavior

* Which Altinity Stable Build version to use 
* Which interface to use, if it matters
* Non-default settings, if any
* `CREATE TABLE` statements for all tables involved
* Sample data for all these tables, use the [clickhouse-obfuscator](https://github.com/ClickHouse/ClickHouse/blob/31fd4f5eb41d5ec26724fc645c11fe4d62eae07f/programs/obfuscator/README.md) if necessary
* Queries to run that lead to an unexpected result

## Expected behavior
A clear, concise description of what you expected to happen.

## Logs, error messages, stacktraces, screenshots...
Add any details that might explain the issue.

## Additional context
Add any other context about the issue here.
