-- The classifier functions read a dictionary whose contents can change on reload, so they are non-deterministic
-- (like dictGet) and must not be constant-folded or reused across executions. naiveBayesNgrams is a pure
-- tokenizer with no dictionary, so it stays deterministic.
SELECT name, deterministic
FROM system.functions
WHERE name IN ('naiveBayesClassifier', 'naiveBayesClassifierWithProb', 'naiveBayesClassifierWithAllProbs', 'naiveBayesNgrams')
ORDER BY name;
