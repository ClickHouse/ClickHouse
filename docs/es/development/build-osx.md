---
machine_translated: true
machine_translated_rev: 3e185d24c9fe772c7cf03d5475247fb829a21dfa
toc_priority: 65
toc_title: "C\xF3mo crear ClickHouse en Mac OS X"
---

# Cómo Crear ClickHouse En Mac OS X {#how-to-build-clickhouse-on-mac-os-x}

Build debería funcionar en Mac OS X 10.15 (Catalina)

## Instalar Homebrew {#install-homebrew}

``` bash
$ /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
```

## Instalar Compiladores, Herramientas y Bibliotecas Necesarios {#install-required-compilers-tools-and-libraries}

``` bash
$ brew install cmake ninja libtool gettext
```

## Fuentes De ClickHouse De Pago {#checkout-clickhouse-sources}

``` bash
$ git clone --recursive git@github.com:ClickHouse/ClickHouse.git
```

o

``` bash
$ git clone --recursive https://github.com/ClickHouse/ClickHouse.git

$ cd ClickHouse
```

## Construir ClickHouse {#build-clickhouse}

``` bash
$ mkdir build
$ cd build
$ cmake .. -DCMAKE_CXX_COMPILER=`which clang++` -DCMAKE_C_COMPILER=`which clang`
$ ninja
$ cd ..
```

## Advertencia {#caveats}

Si tiene la intención de ejecutar clickhouse-server, asegúrese de aumentar la variable maxfiles del sistema.

!!! info "Nota"
    Tendrás que usar sudo.

Para ello, cree el siguiente archivo:

/Library/LaunchDaemons/limit.maxfiles.lista:

``` xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
        "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
  <dict>
    <key>Label</key>
    <string>limit.maxfiles</string>
    <key>ProgramArguments</key>
    <array>
      <string>launchctl</string>
      <string>limit</string>
      <string>maxfiles</string>
      <string>524288</string>
      <string>524288</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>ServiceIPC</key>
    <false/>
  </dict>
</plist>
```

Ejecute el siguiente comando:

``` bash
$ sudo chown root:wheel /Library/LaunchDaemons/limit.maxfiles.plist
```

Reiniciar.

Para verificar si está funcionando, puede usar `ulimit -n` comando.

[Artículo Original](https://clickhouse.tech/docs/en/development/build_osx/) <!--hide-->
