set allow_experimental_json_type = 1;
create table test (json JSON(SKIP REGEXP '[]')) engine=Memory(); -- {serverError BAD_ARGUMENTS}
create table test (json JSON(SKIP REGEXP '+')) engine=Memory(); -- {serverError BAD_ARGUMENTS};

