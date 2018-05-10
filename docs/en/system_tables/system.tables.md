# system.tables

This table contains the String columns 'database', 'name', and 'engine'.
The table also contains three virtual columns: metadata_modification_time (DateTime type), create_table_query, and engine_full (String type).
Each table that the server knows about is entered in the 'system.tables' table.
This system table is used for implementing SHOW TABLES queries.

