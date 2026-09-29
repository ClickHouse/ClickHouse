create table test (json JSON(SKIP REGEXP '[]')) engine=Memory(); -- {serverError CANNOT_COMPILE_REGEXP}
create table test (json JSON(SKIP REGEXP '+')) engine=Memory(); -- {serverError CANNOT_COMPILE_REGEXP};
