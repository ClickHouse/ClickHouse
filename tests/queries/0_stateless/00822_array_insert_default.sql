SELECT sum(ignore(*)) FROM (SELECT arrayFirst(x -> empty(x), [[number]]) FROM numbers(10000000));
