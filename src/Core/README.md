Contains foundation classes to build the database logic.

In general these classes are used everywhere in the database code so any changes here will have an impact throughout the
whole codebase. Some recommendations:
* Prefer forward declarations.
* Avoid templates when possible. When not, prefer explicit instantiations.
* Keep implementation details to `.cpp` files.
* It is preferable to have multiple `.hpp` files to handle subclasses or enum classes if this helps reduce dependencies
in files such as `Settings.h`, `Field.h`, `Block.h`, etc.