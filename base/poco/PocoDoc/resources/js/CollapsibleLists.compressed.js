/*

CollapsibleLists.js

An object allowing lists to dynamically expand and collapse

Created by Stephen Morley - http://code.stephenmorley.org/ - and released under
the terms of the CC0 1.0 Universal legal code:

http://creativecommons.org/publicdomain/zero/1.0/legalcode

Modified by Guenter Obiltschnig (added expansion via URI query string)

*/

var CollapsibleLists=new function(){function e(e){var t=e.getElementsByTagName("ul");for(var n=0;n<t.length;n++){var r=t[n];while(r.nodeName!="LI")r=r.parentNode;if(r==e)t[n].style.display="block"}e.className=e.className.replace(/(^| )collapsibleList(Open|Closed)( |$)/,"");if(t.length>0){e.className+=" collapsibleList"+(open?"Open":"Closed")}}function t(e){return function(t){if(!t)t=window.event;var r=t.target?t.target:t.srcElement;while(r.nodeName!="LI")r=r.parentNode;if(r==e)n(e)}}function n(e){var t=e.className.match(/(^| )collapsibleListClosed( |$)/);var n=e.getElementsByTagName("ul");for(var r=0;r<n.length;r++){var i=n[r];while(i.nodeName!="LI")i=i.parentNode;if(i==e)n[r].style.display=t?"block":"none"}e.className=e.className.replace(/(^| )collapsibleList(Open|Closed)( |$)/,"");if(n.length>0){e.className+=" collapsibleList"+(t?"Open":"Closed")}}function r(e){e=e.replace(/[\[]/,"\\[").replace(/[\]]/,"\\]");var t=new RegExp("[\\?&]"+e+"=([^&#]*)"),n=t.exec(location.search);return n===null?"":decodeURIComponent(n[1].replace(/\+/g," "))}this.apply=function(t){var n=document.getElementsByTagName("ul");for(var i=0;i<n.length;i++){if(n[i].className.match(/(^| )collapsibleList( |$)/)){this.applyTo(n[i],true);if(!t){var s=n[i].getElementsByTagName("ul");for(var o=0;o<s.length;o++){s[o].className+=" collapsibleList"}}}var u=r("expand");if(u){var a=document.getElementById(u);if(a){e(a)}}}};this.applyTo=function(e,r){var i=e.getElementsByTagName("li");for(var s=0;s<i.length;s++){if(!r||e==i[s].parentNode){if(i[s].addEventListener){i[s].addEventListener("mousedown",function(e){e.preventDefault()},false)}else{i[s].attachEvent("onselectstart",function(){event.returnValue=false})}if(i[s].addEventListener){i[s].addEventListener("click",t(i[s]),false)}else{i[s].attachEvent("onclick",t(i[s]))}n(i[s])}}}}