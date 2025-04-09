-- Tags: no-fasttest
-- no-fasttest: depends on model binary and model details via config files

SELECT naiveBayesClassifier('sentiment3', 'The interface is beautiful and intuitive');
SELECT naiveBayesClassifier('sentiment3', 'This product is amazing in every way');
SELECT naiveBayesClassifier('sentiment3', 'I am impressed by the excellent quality');
SELECT naiveBayesClassifier('sentiment3', 'The app is awful and barely works');
SELECT naiveBayesClassifier('sentiment3', 'Customer support was horrible today');
SELECT naiveBayesClassifier('sentiment3', 'This experience was a total disaster');
