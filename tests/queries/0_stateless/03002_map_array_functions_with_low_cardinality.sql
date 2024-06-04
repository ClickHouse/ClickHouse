SELECT mapContainsKeyLike(map('aa', toLowCardinality(1), 'bb', toLowCardinality(2)), toLowCardinality('a%'));
