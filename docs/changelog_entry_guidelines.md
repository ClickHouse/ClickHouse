# Changelog entry guidelines

Good changelog entries help users quickly understand what's new and how it affects them. We ask contributors to fill out a user-readable changelog entry that will go into the changelog of each release.

Below we discuss a few simple guidelines to writing a good changelog entry.

## Write with the user in mind, not the developer

The changelog entry is aimed at communicating the change to the _user_, and not only to the _developer_.
When writing the entry, where appropriate, try to communicate not just **_what_** changed but also **_why_** this change is useful for the user, or **_how_** it affects the user.

For example, instead of:

> Adds `system.iceberg_history` table

Write:

> Users can now view historical snapshots of Iceberg tables using the new `system.iceberg_history` table.

Instead of:

> Add `stringBytesUniq` and `stringBytesEntropy` functions to search for possibly random or encrypted data."

Write:

> You can now detect potentially encrypted or random data in your strings using the new `stringBytesUniq` and `stringBytesEntropy` functions, helping identify data quality issues or security concerns.

## Keep it simple

Avoid technical jargon a user might not understand without explanation. Aim for between 1-5 sentences, and
don't be afraid to use an LLM to help catch typos, grammar mistakes or to rephrase the entry for you in a 
user friendly way (it's not cheating, I promise!)

Instead of:

> Support correlated subqueries as an argument of `EXISTS` expression

Write:

> You can now use subqueries that reference outer query columns within `EXISTS` clauses.

An example of a clear, simple entry:

> Makes page cache settings adjustable on a per-query level. This is needed for faster experimentation and for the possibility of fine-tuning for high-throughput and low-latency queries.

## Follow a few simple formatting guidelines

### Write in full sentences and in the present tense

Instead of:

> Fixed a crash: if an exception is thrown in an attempt to remove a temporary file

Write:

> Fixes a crash where an exception is thrown in an attempt to remove a temporary file.

### Use backticks where necessary

Backtick code elements like settings, function names, SQL statements, format names, and data types. Generally,
anything you would type into clickhouse-client should be backticked. This helps to make the changelog entries more readable.

Instead of:

> Settings use_skip_indexes_if_final and use_skip_indexes_if_final_exact_mode now default to True

Write:

> Settings `use_skip_indexes_if_final` and `use_skip_indexes_if_final_exact_mode` now default to `True`

### Try to follow a consistent format

Try to follow the same format: What it does → Why it matters to the user → How to use it (if needed). This makes entries scannable and predictable for readers.

For example:

> You can now filter vector search results either before or after the search operation, giving you better control over performance vs. accuracy tradeoffs. Use the new `vector_search_filter_mode` setting to choose your preferred approach.
