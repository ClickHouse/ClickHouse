# 在 Mac OS X 中编译 ClickHouse {#zai-mac-os-x-zhong-bian-yi-clickhouse}

ClickHouse 支持在 Mac OS X 10.12 版本中编译。若您在用更早的操作系统版本，可以尝试在指令中使用 `Gentoo Prefix` 和 `clang sl`.
通过适当的更改，它应该可以适用于任何其他的 Linux 发行版。

## 安装 Homebrew {#an-zhuang-homebrew}

``` bash
$ /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
```

## 安装编译器，工具库 {#an-zhuang-bian-yi-qi-gong-ju-ku}

``` bash
$ brew install cmake ninja libtool gettext
```

## 拉取 ClickHouse 源码 {#la-qu-clickhouse-yuan-ma}

``` bash
git clone --recursive git@github.com:ClickHouse/ClickHouse.git
# or: git clone --recursive https://github.com/ClickHouse/ClickHouse.git

cd ClickHouse
```

## 编译 ClickHouse {#bian-yi-clickhouse}

``` bash
$ mkdir build
$ cd build
$ cmake .. -DCMAKE_CXX_COMPILER=`which clang++` -DCMAKE_C_COMPILER=`which clang`
$ ninja
$ cd ..
```

## 注意事项 {#zhu-yi-shi-xiang}

若你想运行 clickhouse-server，请先确保增加系统的最大文件数配置。

!!! 注意 "注意"
    可能需要用 sudo

为此，请创建以下文件：

/资源库/LaunchDaemons/limit.maxfiles.plist:

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

[来源文章](https://clickhouse.tech/docs/en/development/build_osx/) <!--hide-->
