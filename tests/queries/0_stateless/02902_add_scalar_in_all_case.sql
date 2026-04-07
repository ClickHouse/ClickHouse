SELECT count() FROM format(TSVRaw, (SELECT cast(arrayStringConcat(groupArray('some long string'), '\n'), 'LowCardinality(String)') FROM numbers(10000))) FORMAT TSVRaw;
