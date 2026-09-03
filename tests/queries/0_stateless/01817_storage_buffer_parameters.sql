drop table if exists data_01817;
drop table if exists buffer_01817;

create table data_01817 (key Int) Engine=Null();

-- w/ flush_*
create table buffer_01817 (key Int) Engine=Buffer(currentDatabase(), data_01817,
    /* num_layers= */ 1,
    /* min_time= */   1,     /* max_time= */  86400,
    /* min_rows= */   1e9,   /* max_rows= */  1e6,
    /* min_bytes= */  0,     /* max_bytes= */ 4e6,
    /* flush_time= */ 86400, /* flush_rows= */ 10, /* flush_bytes= */0
);
drop table buffer_01817;

-- w/o flush_*
create table buffer_01817 (key Int) Engine=Buffer(currentDatabase(), data_01817,
    /* num_layers= */ 1,
    /* min_time= */   1,     /* max_time= */  86400,
    /* min_rows= */   1e9,   /* max_rows= */  1e6,
    /* min_bytes= */  0,     /* max_bytes= */ 4e6
);
drop table buffer_01817;

-- not enough args
create table buffer_01817 (key Int) Engine=Buffer(currentDatabase(), data_01817,
    /* num_layers= */ 1,
    /* min_time= */   1,     /* max_time= */  86400,
    /* min_rows= */   1e9,   /* max_rows= */  1e6,
    /* min_bytes= */  0      /* max_bytes= 4e6  */
); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
-- too much args
create table buffer_01817 (key Int) Engine=Buffer(currentDatabase(), data_01817,
    /* num_layers= */ 1,
    /* min_time= */   1,     /* max_time= */  86400,
    /* min_rows= */   1e9,   /* max_rows= */  1e6,
    /* min_bytes= */  0,     /* max_bytes= */ 4e6,
    /* flush_time= */ 86400, /* flush_rows= */ 10, /* flush_bytes= */0,
    0
); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

drop table data_01817;
