Style guide for the util folder is a stricter version of general style guide (mostly in terms of ambiguity resolution).

 * all {} must be in K&R style
 * &, * tied closer to a type, not to variable
 * always use `using` not `typedef`
 * even a single line block must be in braces {}:
   ```
   if (A) {
       B();
   }
   ```
 * _ at the end of private data member of a class - `First_`, `Second_`
 * every .h file must be accompanied with corresponding .cpp to avoid a leakage and check that it is self contained
 * prohibited to use `printf`-like functions


Things declared in the general style guide, which sometimes are missed:

 * `template <`, not `template<`
 * `noexcept`, not `throw ()` nor `throw()`, not required for destructors
 * indents inside `namespace` same as inside `class`


Requirements for a new code (and for corrections in an old code which involves change of behaviour) in util:

 * presence of UNIT-tests
 * presence of comments in Doxygen style
 * accessors without Get prefix (`Length()`, but not `GetLength()`)

This guide is not a mandatory as there is the general style guide.
Nevertheless if it is not followed, then a next `ya style .` run in the util folder will undeservedly update authors of some lines of code.

Thus before a commit it is recommended to run `ya style .` in the util folder.
