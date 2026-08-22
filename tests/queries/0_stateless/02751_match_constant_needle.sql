select match('default/k8s1', '\\A(?:(?:[-0-9_a-z]+(?:\\.[-0-9_a-z]+)*)/k8s1)\\z');
select match('abc123', '[a-zA-Z]+(?P<num>\\d+)');
