---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 65
toc_title: Comment Construire ClickHouse sur Mac OS X
---

# Comment Construire ClickHouse sur Mac OS X {#how-to-build-clickhouse-on-mac-os-x}

Build devrait fonctionner sur Mac OS X 10.15 (Catalina)

## Installer Homebrew {#install-homebrew}

``` bash
$ /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
```

## Installez les compilateurs, outils et bibliothèques requis {#install-required-compilers-tools-and-libraries}

``` bash
$ brew install cmake ninja libtool gettext
```

## Commander Clickhouse Sources {#checkout-clickhouse-sources}

``` bash
$ git clone --recursive git@github.com:ClickHouse/ClickHouse.git
```

ou

``` bash
$ git clone --recursive https://github.com/ClickHouse/ClickHouse.git

$ cd ClickHouse
```

## Construire ClickHouse {#build-clickhouse}

``` bash
$ mkdir build
$ cd build
$ cmake .. -DCMAKE_CXX_COMPILER=`which clang++` -DCMAKE_C_COMPILER=`which clang`
$ ninja
$ cd ..
```

## Mises en garde {#caveats}

Si vous avez l'intention d'exécuter clickhouse-server, assurez-vous d'augmenter la variable maxfiles du système.

!!! info "Note"
    Vous aurez besoin d'utiliser sudo.

Pour ce faire, créez le fichier suivant:

/ Bibliothèque / LaunchDaemons / limite.maxfiles.plist:

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

Exécutez la commande suivante:

``` bash
$ sudo chown root:wheel /Library/LaunchDaemons/limit.maxfiles.plist
```

Redémarrer.

Pour vérifier si elle fonctionne, vous pouvez utiliser `ulimit -n` commande.

[Article Original](https://clickhouse.tech/docs/en/development/build_osx/) <!--hide-->
