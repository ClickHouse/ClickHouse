#!/bin/env python3

import argparse
import textwrap
import boto3


def folder_list_add(folders, key, value):
    p = key.split('/')
    if len(p) > 1:
        if '' not in folders:
            folders[''] = [set(), dict()]
        folders[''][0].add(p[0])
    for i in range(len(p)-1):
        base = '/'.join(p[:i+1])
        if base not in folders:
            folders[base] = [set(), dict()]
        if i == len(p)-2:
            folders[base][1][p[i+1]] = value
        else:
            folders[base][0].add(p[i+1])


def human_readable_size(size, decimal_places=2):
    for unit in ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB']:
        if size < 1024.0 or unit == 'PiB':
            break
        size /= 1024.0
    return f"{size:.{decimal_places}f} {unit}"


def folderjoin(items, slashes=False):
    result = "/".join([item.strip("/") for item in items if item != "" and item != "/"])
    if slashes:
        if not result.startswith("/"):
            result = "/" + result
        if not result.endswith("/"):
            result = result + "/"
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(dest='fqdn', help="Name of S3 bucket and domain name")
    parser.add_argument(dest='prefix', help="S3 prefix to index")

    args = parser.parse_args()

    if args.prefix.startswith("/"):
        args.prefix = args.prefix[1:]

    if not args.prefix.endswith("/"):
        args.prefix += "/"

    parent_dir_str = "Parent Directory"

    folders = dict()

    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    objs = []
    for page in paginator.paginate(Bucket=args.fqdn, Prefix=args.prefix):
        if 'Contents' in page:
            objs.extend(page['Contents'])

    for o in objs:
        key = o['Key']
        if key.startswith(args.prefix):
            key = key[len(args.prefix):]
        folder_list_add(folders, key, o)

    for folder in folders:
        subs, files = folders[folder]
        folders[folder] = (subs, files, 1 + max(
            [len(parent_dir_str)] +
            [len(s)+1 for s in subs] +
            [len(f) for f in files]
        ))

    for folder in folders:
        print(folder)
        subs, files, maxlen = folders[folder]
        indexdata = list()
        indexdata.append(textwrap.dedent(
            f"""\
            <!DOCTYPE html>
            <html lang="en">
            <head>
                <meta charset="UTF-8">
                <title>Index of {folder}</title>
            </head>
            <body>
            <h1>Index of {folder}</h1>
            <pre>
            <b>{"Name":{maxlen}}   {"Last Modified":20}   {"Size":20}</b>
            """))
        if folder != "":
            parent_folder = folderjoin(folder.split("/")[:-1], slashes=True)
            indexdata.append(f'<a href="{parent_folder}">{parent_dir_str}</a>' +
                             f'{" ":{maxlen-len(parent_dir_str)}}   {"-":20}   {"-":20}\n')
        for sub in subs:
            sub_path = folderjoin([folder, sub], slashes=True)
            indexdata.append(f'<a href="{sub_path}">{sub}/</a>{" ":{maxlen-len(sub)-1}}' +
                             f'   {"-":20}   {"-":20}\n')
        for file in files:
            if file != "index.html":
                file_path = folderjoin([folder, file])
                mtime = files[file]['LastModified']
                size = human_readable_size(files[file]['Size'])
                indexdata.append(f'<a href="/{file_path}">{file}</a>{" ":{maxlen-len(file)}}' +
                                 f'   {mtime:%Y-%m-%d %H:%M:%S}    {size:20}\n')
        indexdata.append("</pre>\n</body>\n</html>\n")
        s3.put_object(
            Bucket=args.fqdn,
            Key=folderjoin([args.prefix, folder, "index.html"]),
            ContentType="text/html",
            Body="".join(indexdata)
        )


if __name__ == "__main__":
    main()
