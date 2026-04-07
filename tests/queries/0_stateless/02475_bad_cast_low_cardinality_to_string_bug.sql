SELECT if(materialize(0), extract(materialize(CAST('aaaaaa', 'LowCardinality(String)')), '\\w'), extract(materialize(CAST('bbbbb', 'LowCardinality(String)')), '\\w*')) AS res FROM numbers(2);
