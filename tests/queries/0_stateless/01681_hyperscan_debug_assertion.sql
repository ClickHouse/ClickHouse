-- We throw our own exception from operator new.
-- In previous versions of Hyperscan it triggered debug assertion as it only expected std::bad_alloc.

SET allow_hyperscan = 1;
SET max_memory_usage = 4000000;
SELECT [1, 2, 3, 11] = arraySort(multiMatchAllIndices('фабрикант', ['', 'рикан', 'а', 'f[ae]b[ei]rl', 'ф[иаэе]б[еэи][рпл]', 'афиукд', 'a[ft],th', '^ф[аиеэ]?б?[еэи]?$', 'берлик', 'fab', 'фа[беьв]+е?[рлко]'])); -- { serverError 241 }
