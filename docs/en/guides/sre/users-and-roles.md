---
sidebar_label: Defining SQL Users and Roles
sidebar_position: 20
---

# Defining SQL Users and Roles

This article shows the basics of defining SQL users and roles then applying privileges and permissions to a databases, tables, rows and columns.

## 1. Enabling SQL user mode and defining users

1.  Enable SQL user mode in the `users.xml` file under the `<default>` user:
    ```xml
    <access_management>1</access_management>
    ```

    :::note
    The `default` user is the only user that gets created with a fresh install and is also the account used for internode communications, by default.

    In production, it is recommended to disable this user once the inter-node commnication has been configured with a SQL admin user and inter-node communications have been set with `<secret>`, cluster credentials and/or internode http and transport protocol credentials since the `default` account is used for internode communication.
    :::

2. Restart the nodes to apply the changes.

3. Start the ClickHouse client:
    ```sql
    clickhouse-client --user default --password <password>
    ```

4. Create a SQL administrator account:
    ```sql
    CREATE USER clickhouse_admin IDENTIFIED WITH plaintext_password BY 'password';
    ```

    :::note
    In this example, a plain text password is used. However, there are several options available for other user directories such as LDAP and Active Directory. Please refer to user guides and documentation for configuring other options.
    :::

5. Grant the new user full administrative rights
    ```sql
    GRANT ALL ON *.* TO clickhouse_admin WITH GRANT OPTION;
    ```

6. Create regular user to restrict columns
    ```sql
    CREATE USER column_user IDENTIFIED WITH plaintext_password BY 'password';
    ```

7. Create a regular user to restrict by row values
    ```sql
    CREATE USER row_user IDENTIFIED WITH plaintext_password BY 'password';
    ```

## 2. Creating a sample database, table and rows

1. Create a test database
    ```sql
    CREATE DATABASE db1;
    ```

2. Create a table
    ```sql
    CREATE TABLE db1.table1 (
        id UInt64,
        column1 String,
        column2 String
    )
    ENGINE MergeTree
    ORDER BY id;
    ```

3. Populate the table with sample rows
    ```sql
    INSERT INTO db1.table1
        (id, column1, column2)
    VALUES
        (1, 'A', 'abc'),
        (2, 'A', 'def'),
        (3, 'B', 'abc'),
        (4, 'B', 'def');
    ```

4. Verify the table:
    ```sql
    SELECT *
    FROM db1.table1
    ```

    ```response
    Query id: 475015cc-6f51-4b20-bda2-3c9c41404e49

    ┌─id─┬─column1─┬─column2─┐
    │  1 │ A       │ abc     │
    │  2 │ A       │ def     │
    │  3 │ B       │ abc     │
    │  4 │ B       │ def     │
    └────┴─────────┴─────────┘
    ```

## 3. Creating roles
With this set of examples, roles for different privileges such as columns and rows will be created, privileges will be granted to the roles and users will be assigned to each role. Roles are used to define groups of users for certain privileges instead of managing each user seperately.
1.  Create a role to restrict users of this role to only see `column1` in database `db1` and `table1`:
    ```sql
    CREATE ROLE column1_users;
    ```

2. Set privileges to allow view on `column1`
    ```sql
    GRANT SELECT(id, column1) ON db1.table1 TO column1_users;
    ```

3. Add the `column_user` user to the `column1_users` role
    ```sql
    GRANT column1_users TO column_user;
    ```

4. Create a role to restrict users of this role to only see selected rows, in this case only rows containing `A` in `column1`
    ```sql
    CREATE ROLE A_rows_users;
    ```

5. Add the `row_user` to the `A_rows_users` role
    ```sql
    GRANT A_rows_users TO row_user;
    ```

5. Create a policy to allow view on only where `column1` has the values of `A`
    ```sql
    CREATE ROW POLICY A_row_filter ON db1.table1 FOR SELECT USING column1 = 'A' TO A_rows_users;
    ```

6. Set privileges to the database and table
    ```sql
    GRANT SELECT(id, column1, column2) ON db1.table1 TO A_rows_users;
    ```

7. grant explicit permissions for other roles to still have access to all rows
    ```sql
    CREATE ROW POLICY allow_other_users_filter ON db1.table1 FOR SELECT USING 1 TO clickhouse_admin, column1_users;
    ```

    :::note
    When attaching a policy to a table, the system will apply that policy and only those users and roles defined will be able to do operations on the table, all others will be denied any operations.
    In order to not have the restrictive row policy applied to other users, another policy must be defined to allow other users and roles to have regular or other types of access.
    :::

## 4. Testing role privileges with column restricted user

1. Log into the clickhouse client using the `clickhouse_admin` user
    ```
    clickhouse-client --user clickhouse_admin --password password
    ```

2. Verify access to database, table and all rows with the admin user.
    ```sql
    SELECT *
    FROM db1.table1
    ```

    ```response
    Query id: f5e906ea-10c6-45b0-b649-36334902d31d

    ┌─id─┬─column1─┬─column2─┐
    │  1 │ A       │ abc     │
    │  2 │ A       │ def     │
    │  3 │ B       │ abc     │
    │  4 │ B       │ def     │
    └────┴─────────┴─────────┘
    ```

3. Log into the ClickHouse client using the `column_user` user 
    ```
    clickhouse-client --user column_user --password password
    ```

4. Test `SELECT` using all columns
    ```sql
    SELECT *
    FROM db1.table1
    ```

    ```response
    Query id: 5576f4eb-7450-435c-a2d6-d6b49b7c4a23


    0 rows in set. Elapsed: 0.006 sec.

    Received exception from server (version 22.3.2):
    Code: 497. DB::Exception: Received from localhost:9000. DB::Exception: column_user: Not enough privileges. To execute this query it's necessary to have grant SELECT(id, column1, column2) ON db1.table1. (ACCESS_DENIED)
    ```

    :::note
    Access is denied since all columns were specified and the user only has access to `id` and `column1`
    :::

4. Verify `SELECT` query with only columns specified and allowed:
    ```sql
    SELECT
        id,
        column1
    FROM db1.table1
    ```

    ```response
    Query id: cef9a083-d5ce-42ff-9678-f08dc60d4bb9

    ┌─id─┬─column1─┐
    │  1 │ A       │
    │  2 │ A       │
    │  3 │ B       │
    │  4 │ B       │
    └────┴─────────┘
    ```

## 5. Testing role privileges with row restricted user

1. Log into the ClickHouse client using `row_user`
    ```
    clickhouse-client --user row_user --password password
    ```

2. View rows available
    ```sql
    SELECT *
    FROM db1.table1
    ```

    ```response
    Query id: a79a113c-1eca-4c3f-be6e-d034f9a220fb

    ┌─id─┬─column1─┬─column2─┐
    │  1 │ A       │ abc     │
    │  2 │ A       │ def     │
    └────┴─────────┴─────────┘
    ```

## 4. Modifying Users and Roles

Users can be assigned multiple roles for a combination of privileges needed. When using multiple roles, the system will combine the roles to determine privileges, the net effect will be that the role permissions will be cumulative.

For example, if one `role1` allows for only select on `column1` and `role2` allows for select on `column1` and `column2` then the user will have access to both columns.

1. Using the admin account, create new user to restrict by both row and column with default roles
    ```sql
    CREATE USER row_and_column_user IDENTIFIED WITH plaintext_password BY 'password' DEFAULT ROLE A_rows_users;
    ```

2. Remove prior privileges for `A_rows_users` role
    ```sql
    REVOKE SELECT(id, column1, column2) ON db1.table1 FROM A_rows_users;
    ```

3. Allow `A_row_users` role to only select from `column1`
    ```sql
    GRANT SELECT(id, column1) ON db1.table1 TO A_rows_users;
    ```

4. Log into the ClickHouse client using `row_and_column_user`
    ```
    clickhouse-client --user row_and_column_user --password password;
    ```

5. Test with all columns:
    ```sql
    SELECT *
    FROM db1.table1
    ```

    ```response
    Query id: 8cdf0ff5-e711-4cbe-bd28-3c02e52e8bc4


    0 rows in set. Elapsed: 0.005 sec.

    Received exception from server (version 22.3.2):
    Code: 497. DB::Exception: Received from localhost:9000. DB::Exception: row_and_column_user: Not enough privileges. To execute this query it's necessary to have grant SELECT(id, column1, column2) ON db1.table1. (ACCESS_DENIED)
    ```

6. Test with limited allowed columns:
    ```sql
    SELECT
        id,
        column1
    FROM db1.table1
    ```

    ```response
    Query id: 5e30b490-507a-49e9-9778-8159799a6ed0

    ┌─id─┬─column1─┐
    │  1 │ A       │
    │  2 │ A       │
    └────┴─────────┘
    ```

7. Examples on how to delete privileges, policies, unassign users from roles, delete users and roles:
    * Remove privilege from a role
    ```sql
    REVOKE SELECT(column1, id) ON db1.table1 FROM A_rows_users;
    ```

    * Delete a policy
    ```sql
    DROP ROW POLICY A_row_filter ON db1.table1;
    ```

    * Unassign a user from a role
    ```sql
    REVOKE A_rows_users FROM row_user;
    ```

    * Delete a role
    ```sql
    DROP ROLE A_rows_users;
    ```

    * Delete a user
    ```sql
    DROP USER row_user;
    ```

## 5. Troubleshooting

1. There are occasions when privileges intersect or combine to produce unexpected results, the following commands can be used to narrow the issue using an admin account
    * Listing the grants and roles for a user
    ```sql
    SHOW GRANTS FOR row_and_column_user
    ```

    ```response
    Query id: 6a73a3fe-2659-4aca-95c5-d012c138097b

    ┌─GRANTS FOR row_and_column_user───────────────────────────┐
    │ GRANT A_rows_users, column1_users TO row_and_column_user │
    └──────────────────────────────────────────────────────────┘
    ```

    * List roles in ClickHouse
    ```sql
    SHOW ROLES
    ```

    ```response
    Query id: 1e21440a-18d9-4e75-8f0e-66ec9b36470a

    ┌─name────────────┐
    │ A_rows_users    │
    │ column1_users   │
    └─────────────────┘
    ```

    * Display the policies
    ```sql
    SHOW ROW POLICIES
    ```

    ```response
    Query id: f2c636e9-f955-4d79-8e80-af40ea227ebc

    ┌─name───────────────────────────────────┐
    │ A_row_filter ON db1.table1             │
    │ allow_other_users_filter ON db1.table1 │
    └────────────────────────────────────────┘
    ```

    * View how a policy was defined and current privileges
    ```sql
    SHOW CREATE ROW POLICY A_row_filter ON db1.table1
    ```

    ```response
    Query id: 0d3b5846-95c7-4e62-9cdd-91d82b14b80b

    ┌─CREATE ROW POLICY A_row_filter ON db1.table1────────────────────────────────────────────────┐
    │ CREATE ROW POLICY A_row_filter ON db1.table1 FOR SELECT USING column1 = 'A' TO A_rows_users │
    └─────────────────────────────────────────────────────────────────────────────────────────────┘
    ```


## Summary

This article demostrated the basics of creating SQL users and roles and provided steps to set and modify privileges for users and roles.
For more detailed information on each please refer to our user guides and reference documenation.
