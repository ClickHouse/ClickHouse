Contains foundation classes to build the database logic.

In general these classes are used everywhere in the database code so any changes here will have an impact throughout the
whole codebase. Some recommendations:
* Be extra careful what you put in here. The contained code should be useful in multiple places and be high-quality.
* Prefer forward declarations.
* Avoid templates when possible. When not, prefer explicit instantiations.
* Keep implementation details to `.cpp` files.
* It is preferable to have multiple `.h` files to handle subclasses or enum classes if this helps reduce dependencies
in files such as `Settings.h`, `Field.h`, `Block.h`, etc.
