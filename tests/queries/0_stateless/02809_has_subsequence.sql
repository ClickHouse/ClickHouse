select hasSubsequence('garbage', '');
select hasSubsequence('garbage', 'g');
select hasSubsequence('garbage', 'a');
select hasSubsequence('garbage', 'e');
select hasSubsequence('garbage', 'gr');
select hasSubsequence('garbage', 'ab');
select hasSubsequence('garbage', 'be');
select hasSubsequence('garbage', 'arg');
select hasSubsequence('garbage', 'garbage');

select hasSubsequence('garbage', 'garbage1');
select hasSubsequence('garbage', 'arbw');
select hasSubsequence('garbage', 'ARG');

select hasSubsequenceCaseInsensitive('garbage', 'ARG');

select hasSubsequence(materialize('garbage'), materialize(''));
select hasSubsequence(materialize('garbage'), materialize('arg'));
select hasSubsequence(materialize('garbage'), materialize('garbage1'));