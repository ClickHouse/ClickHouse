## Example()

This function serves as an example of what function documentation should look like.

**Syntax**

```sql
Example(input, secondary_input)
```

**Arguments**

- `input`: This is an example input. [String literal](../syntax#syntax-string-literal)
- `secondary_input`: This is another example input. [Expression](../syntax#syntax-expressions)

**Returned value**

This function doesn't exists. But if it did, they it would return a bool.

**Implementation details**

**Example**

Query:

```sql
CREATE TABLE example_table
(
    id UUID,
    name String,
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO example_table VALUES (generateUUIDv4(), Example());
SELECT * FROM example_table;
```

**Result**:

```response
9e22db8c-d343-4bc5-8a95-501f635c0fc4	Example
```
