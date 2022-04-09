---
sidebar_label: Updating and Deleting Data
sidebar_position: 99
keywords: [update, delete, mutation]
---

# Updating and Deleting ClickHouse Data

Although ClickHouse is geared toward high volume analytic workloads, it is possible in some situations to modify or delete existing data.  These operations are labeled "mutations" and are executed using the `ALTER TABLE` command.

## Updating Existing Data

From ClickHouse client, enter your update `ALTER TABLE` command in this form:

```sql
ALTER TABLE [<database>.]<table> UPDATE <column> = <expression> WHERE <filter_expr>
```

`<expression>` is the new value for the column where the `<filter_expr>` is satisfied.  The `<expression>` must be the same datatype as the column or be convertable to the same datatype using the `CAST` operator.  The `<filter_expr>` should return a UInt8 (zero or non-zero) value for each row of the data.  Multiple `UPDATE <column>` statements can be combined in a single `ALTER TABLE` command separated by commas.

**Examples**:

 1.  A mutation like this allows updating replacing visitor_ids with new ones using a dictionary lookup:

     ```sql
    ALTER TABLE website.clicks UPDATE visitor_id = getDict('visitors', 'new_visitor_id', visitor_id) WHERE visit_date < '2022-01-01'
    ```

     
2.   Modifying multiple values in one command can be more efficient than multiple commands:

     ```sql
     ALTER TABLE website.clicks UPDATE url = substring(url, position(url, '://') + 3), visitor_id = new_visit_id WHERE visit_date < '2022-01-01'
     ```

3.  Mutations can be exectuted `ON CLUSTER` for sharded tables:

     ```sql
     ALTER TABLE clicks ON CLUSTER main_cluster UPDATE click_count = click_count / 2 WHERE visitor_id ILIKE '%robot%'
     ```

**Note**

* It is not possible to update columns that are part of the primary or sorting key.



## Deleting Data 

From ClickHouse client, enter your delete `ALTER TABLE` command in this form:
```sql
ALTER TABLE [<database>.]<table> DELETE WHERE <filter_expr>
```

Again `<filter_expr>` should return a UInt8 value for each row of data.

**Examples**

1. Delete any records where a column is in an array of values: 
    ```sql
    ALTER TABLE website.clicks DELETE WHERE visitor_id in (253, 1002, 4277)
    ```

2.  What does this query alter?
    ```sql
    ALTER TABLE clicks ON CLUSTER main_cluster WHERE visit_date < '2022-01-02 15:00:00' AND page_id = '573'
    ```

**Note**

To delete all of the data in a table, it is more efficient to use the command `TRUNCATE TABLE [<database].]<table>` command.  This command can also be executed `ON CLUSTER`.





