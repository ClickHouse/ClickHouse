## How it was created?

```
ninja -t deps | grep -P '^    /' | sort | uniq | grep '/usr/include/' | grep -vF 'c++' | grep -vP 'llvm|unicode|readline|hs' | sed 's/    \/usr\/include\///' | xargs -I{} cp -r /usr/include/{} ~/ClickHouse/contrib/libc-headers/{}
```

```
$ ldd --version
ldd (Ubuntu GLIBC 2.27-3ubuntu1) 2.27
```

## Why do we need this?

These files are needed for more consistent builds.
