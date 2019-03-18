# 在 Mac OS X 中编译 ClickHouse

ClickHouse 支持在 Mac OS X 10.12 版本中编译。若您在用更早的操作系统版本，可以尝试在指令中使用 `Gentoo Prefix` 和 `clang sl`.
通过适当的更改，它应该可以适用于任何其他的 Linux 发行版。

## 安装 Homebrew

```bash
/usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
```

## 安装编译器，工具库

```bash
brew install cmake ninja gcc icu4c mariadb-connector-c openssl libtool gettext readline
```

## 拉取 ClickHouse 源码

```bash
git clone --recursive git@github.com:yandex/ClickHouse.git
# or: git clone --recursive https://github.com/yandex/ClickHouse.git

cd ClickHouse
```

For the latest stable version, switch to the `stable` branch.

## 编译 ClickHouse

```bash
mkdir build
cd build
cmake .. -DCMAKE_CXX_COMPILER=`which g++-8` -DCMAKE_C_COMPILER=`which gcc-8`
ninja
cd ..
```

## 注意事项

若你想运行 clickhouse-server，请先确保增加系统的最大文件数配置。

!!! 注意
    可能需要用 sudo

为此，请创建以下文件：

/Library/LaunchDaemons/limit.maxfiles.plist:
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

执行以下命令：
``` bash
$ sudo chown root:wheel /Library/LaunchDaemons/limit.maxfiles.plist
```

然后重启。

可以通过 `ulimit -n` 命令来检查是否生效。


[来源文章](https://clickhouse.yandex/docs/en/development/build_osx/) <!--hide-->
