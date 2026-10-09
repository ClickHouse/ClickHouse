-- Tags: no-fasttest
-- no-fasttest: depends on model binary and model details via config files

SELECT naiveBayesClassifier('sentiment_token_1', 'The interface is beautiful and intuitive');
SELECT naiveBayesClassifier('sentiment_token_1', 'This product is amazing in every way');
SELECT naiveBayesClassifier('sentiment_token_1', 'I am impressed by the excellent quality');
SELECT naiveBayesClassifier('sentiment_token_1', 'The app is awful and barely works');
SELECT naiveBayesClassifier('sentiment_token_1', 'Customer support was horrible today');
SELECT naiveBayesClassifier('sentiment_token_1', 'This experience was a total disaster');
