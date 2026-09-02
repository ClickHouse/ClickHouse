create table test (number UInt64) engine=FileLog('./user_files/data.jsonl', 'JSONEachRow') settings poll_max_batch_size=18446744073709; -- {serverError INVALID_SETTING_VALUE}

