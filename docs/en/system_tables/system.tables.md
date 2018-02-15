# system.tables

This table contains the String columns 'database', 'name', and 'engine'.
Also, the table has three virtual columns: metadata_modification_time of type DateTime, create_table_query and engine_full of type String.
Each table that the server knows about is entered in the 'system.tables' table.
This system table is used for implementing SHOW TABLES queries.
