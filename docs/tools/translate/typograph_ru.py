import requests

class TypographError(Exception):
    pass


def typograph(text):
    text = text.replace('&', '&amp;')
    text = text.replace('<', '&lt;')
    text = text.replace('>', '&gt;')
    template = f'''<?xml version="1.0" encoding="UTF-8"?>
    <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema"
        xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
    <soap:Body>
    <ProcessText xmlns="http://typograf.artlebedev.ru/webservices/">
    <text>{text}</text>
         <entityType>3</entityType>
         <useBr>0</useBr>
         <useP>0</useP>
         <maxNobr>0</maxNobr>
    </ProcessText>
    </soap:Body>
    </soap:Envelope>
    '''
    result = requests.post(
        url='http://typograf.artlebedev.ru/webservices/typograf.asmx',
        data=template.encode('utf-8'),
        headers={
            'Content-Type': 'text/xml',
            'SOAPAction': 'http://typograf.artlebedev.ru/webservices/ProcessText'
        }
    )
    if result.ok and 'ProcessTextResult' in result.text:
        result_text = result.text.split('<ProcessTextResult>')[1].split('</ProcessTextResult>')[0].rstrip()
        result_text = result_text.replace('&amp;', '&')
        result_text = result_text.replace('&lt;', '<')
        result_text = result_text.replace('&gt;', '>')
        return result_text
    else:
        raise TypographError(result.text)


if __name__ == '__main__':
    import sys
    print((typograph(sys.stdin.read())))
