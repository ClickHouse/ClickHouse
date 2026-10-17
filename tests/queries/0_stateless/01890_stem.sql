-- Tags: no-fasttest
-- Tag no-fasttest: depends on libstemmer_c

SET allow_experimental_nlp_functions = 1;

SELECT stem('en', 'given');
SELECT stem('en', 'combinatorial');
SELECT stem('en', 'collection');
SELECT stem('en', 'possibility');
SELECT stem('en', 'studied');
SELECT stem('en', 'commonplace');
SELECT stem('en', 'packing');

SELECT stem('ru', 'комбинаторной');
SELECT stem('ru', 'получила');
SELECT stem('ru', 'ограничена');
SELECT stem('ru', 'конечной');
SELECT stem('ru', 'максимальной');
SELECT stem('ru', 'суммарный');
SELECT stem('ru', 'стоимостью');

SELECT stem('fr', 'remplissage');
SELECT stem('fr', 'valeur');
SELECT stem('fr', 'maximiser');
SELECT stem('fr', 'dépasser');
SELECT stem('fr', 'intensivement');
SELECT stem('fr', 'étudié');
SELECT stem('fr', 'peuvent');
