# system.tables

This table contains the String columns 'database', 'name', and 'engine' and the DateTime column 'metadata_modification_time'.
Each table that the server knows about is entered in the 'system.tables' table.
There is an issue: table engines are specified without parameters.
This system table is used for implementing SHOW TABLES queries.

