/*
 * File: iframeReizer.js
 * Desc: Force iframes to size to content.
 * Requires: iframeResizer.contentWindow.js to be loaded into the target frame.
 * Author: David J. Bradshaw - dave@bradshaw.net
 * Contributor: Jure Mav - jure.mav@gmail.com
 */
;(function() {
    'use strict';

	var
		count                 = 0,
		firstRun              = true,
		msgHeader             = 'message',
		msgHeaderLen          = msgHeader.length,
		msgId                 = '[iFrameSizer]', //Must match iframe msg ID
		msgIdLen              = msgId.length,
		page                  =  '', //:'+location.href, //Uncoment to debug nested iFrames
		pagePosition          = null,
		requestAnimationFrame = window.requestAnimationFrame,
		resetRequiredMethods  = {max:1,scroll:1,bodyScroll:1,documentElementScroll:1},
		settings              = {},

		defaults              = {
			autoResize                : true,
			bodyBackground            : null,
			bodyMargin                : null,
			bodyMarginV1              : 8,
			bodyPadding               : null,
			checkOrigin               : true,
			enablePublicMethods       : false,
			heightCalculationMethod   : 'offset',
			interval                  : 32,
			log                       : false,
			maxHeight                 : Infinity,
			maxWidth                  : Infinity,
			minHeight                 : 0,
			minWidth                  : 0,
			scrolling                 : false,
			sizeHeight                : true,
			sizeWidth                 : false,
			tolerance                 : 0,
			closedCallback            : function(){},
			initCallback              : function(){},
			messageCallback           : function(){},
			resizedCallback           : function(){}
		};

	function addEventListener(obj,evt,func){
		if ('addEventListener' in window){
			obj.addEventListener(evt,func, false);
		} else if ('attachEvent' in window){//IE
			obj.attachEvent('on'+evt,func);
		}
	}

	function setupRequestAnimationFrame(){
		var
			vendors = ['moz', 'webkit', 'o', 'ms'],
			x;

		// Remove vendor prefixing if prefixed and break early if not
		for (x = 0; x < vendors.length && !requestAnimationFrame; x += 1) {
			requestAnimationFrame = window[vendors[x] + 'RequestAnimationFrame'];
		}

		if (!(requestAnimationFrame)){
			log(' RequestAnimationFrame not supported');
		}
	}

	function log(msg){
		if (settings.log && (typeof console === 'object')){
			console.log(msgId + '[Host page'+page+']' + msg);
		}
	}


	function iFrameListener(event){
		function resizeIFrame(){
			function resize(){
				setSize(messageData);
				setPagePosition();
				settings.resizedCallback(messageData);
			}

			syncResize(resize,messageData,'resetPage');
		}

		function closeIFrame(iframe){
			var iframeID = iframe.id;

			log(' Removing iFrame: '+iframeID);
			iframe.parentNode.removeChild(iframe);
			settings.closedCallback(iframeID);
			log(' --');
		}

		function processMsg(){
			var data = msg.substr(msgIdLen).split(':');

			return {
				iframe: document.getElementById(data[0]),
				id:     data[0],
				height: data[1],
				width:  data[2],
				type:   data[3]
			};
		}

		function ensureInRange(Dimension){
			var
				max  = Number(settings['max'+Dimension]),
				min  = Number(settings['min'+Dimension]),
				dimension = Dimension.toLowerCase(),
				size = Number(messageData[dimension]);

			if (min>max){
				throw new Error('Value for min'+Dimension+' can not be greater than max'+Dimension);
			}

			log(' Checking '+dimension+' is in range '+min+'-'+max);

			if (size<min) {
				size=min;
				log(' Set '+dimension+' to min value');
			}

			if (size>max) {
				size=max;
				log(' Set '+dimension+' to max value');
			}

			messageData[dimension]=''+size;
		}

		function isMessageFromIFrame(){

			var
				origin     = event.origin,
				remoteHost = messageData.iframe.src.split('/').slice(0,3).join('/');

			if (settings.checkOrigin) {
				log(' Checking connection is from: '+remoteHost);

				if ((''+origin !== 'null') && (origin !== remoteHost)) {
					throw new Error(
						'Unexpected message received from: ' + origin +
						' for ' + messageData.iframe.id +
						'. Message was: ' + event.data +
						'. This error can be disabled by adding the checkOrigin: false option.'
					);
				}
			}

			return true;
		}

		function isMessageForUs(){
			return msgId === ('' + msg).substr(0,msgIdLen); //''+Protects against non-string msg
		}

		function isMessageFromMetaParent(){
			//test if this message is from a parent above us. This is an ugly test, however, updating
			//the message format would break backwards compatibity.
			var retCode = messageData.type in {'true':1,'false':1};

			if (retCode){
				log(' Ignoring init message from meta parent page');
			}

			return retCode;
		}

		function forwardMsgFromIFrame(){
			var msgBody = msg.substr(msg.indexOf(':')+msgHeaderLen+6); //6 === ':0:0:' + ':' (Ideas to name this magic number most welcome)

			log(' MessageCallback passed: {iframe: '+ messageData.iframe.id + ', message: ' + msgBody + '}');
			settings.messageCallback({
				iframe: messageData.iframe,
				message: msgBody
			});
			log(' --');
		}

		function checkIFrameExists(){
			if (null === messageData.iframe) {
				throw new Error('iFrame ('+messageData.id+') does not exist on ' + page);
			}
			return true;
		}

		function scrollRequestFromChild(){
			log(' Reposition requested from iFrame');
			pagePosition = {
				x: messageData.width,
				y: messageData.height
			};
			setPagePosition();
		}

		function actionMsg(){
			switch(messageData.type){
				case 'close':
					closeIFrame(messageData.iframe);
					settings.resizedCallback(messageData); //To be removed.
					break;
				case 'message':
					forwardMsgFromIFrame();
					break;
				case 'scrollTo':
					scrollRequestFromChild();
					break;
				case 'reset':
					resetIFrame(messageData);
					break;
				case 'init':
					resizeIFrame();
					settings.initCallback(messageData.iframe);
					break;
				default:
					resizeIFrame();
			}
		}

		var
			msg = event.data,
			messageData = {};

		if (isMessageForUs()){
			log(' Received: '+msg);
			messageData = processMsg();
			ensureInRange('Height');
			ensureInRange('Width');

			if ( !isMessageFromMetaParent() && checkIFrameExists() && isMessageFromIFrame() ){
				actionMsg();
				firstRun = false;
			}
		}
	}


	function getPagePosition (){
		if(null === pagePosition){
			pagePosition = {
				x: (window.pageXOffset !== undefined) ? window.pageXOffset : document.documentElement.scrollLeft,
				y: (window.pageYOffset !== undefined) ? window.pageYOffset : document.documentElement.scrollTop
			};
			log(' Get position: '+pagePosition.x+','+pagePosition.y);
		}
	}

	function setPagePosition(){
		if(null !== pagePosition){
			window.scrollTo(pagePosition.x,pagePosition.y);
			log(' Set position: '+pagePosition.x+','+pagePosition.y);
			pagePosition = null;
		}
	}

	function resetIFrame(messageData){
		function reset(){
			setSize(messageData);
			trigger('reset','reset',messageData.iframe);
		}

		log(' Size reset requested by '+('init'===messageData.type?'host page':'iFrame'));
		getPagePosition();
		syncResize(reset,messageData,'init');
	}

	function setSize(messageData){
		function setDimension(dimension){
			messageData.iframe.style[dimension] = messageData[dimension] + 'px';
			log(
				' IFrame (' + messageData.iframe.id +
				') ' + dimension +
				' set to ' + messageData[dimension] + 'px'
			);
		}

		if( settings.sizeHeight) { setDimension('height'); }
		if( settings.sizeWidth ) { setDimension('width'); }
	}

	function syncResize(func,messageData,doNotSync){
		if(doNotSync!==messageData.type && requestAnimationFrame){
			log(' Requesting animation frame');
			requestAnimationFrame(func);
		} else {
			func();
		}
	}

	function trigger(calleeMsg,msg,iframe){
		log('[' + calleeMsg + '] Sending msg to iframe ('+msg+')');
		iframe.contentWindow.postMessage( msgId + msg, '*' );
	}


	function setupIFrame(){
		function setLimits(){
			function addStyle(style){
				if ((Infinity !== settings[style]) && (0 !== settings[style])){
					iframe.style[style] = settings[style] + 'px';
					log(' Set '+style+' = '+settings[style]+'px');
				}
			}

			addStyle('maxHeight');
			addStyle('minHeight');
			addStyle('maxWidth');
			addStyle('minWidth');
		}

		function ensureHasId(iframeID){
			if (''===iframeID){
				iframe.id = iframeID = 'iFrameResizer' + count++;
				log(' Added missing iframe ID: '+ iframeID +' (' + iframe.src + ')');
			}

			return iframeID;
		}

		function setScrolling(){
			log(' IFrame scrolling ' + (settings.scrolling ? 'enabled' : 'disabled') + ' for ' + iframeID);
			iframe.style.overflow = false === settings.scrolling ? 'hidden' : 'auto';
			iframe.scrolling      = false === settings.scrolling ? 'no' : 'yes';
		}

		//The V1 iFrame script expects an int, where as in V2 expects a CSS
		//string value such as '1px 3em', so if we have an int for V2, set V1=V2
		//and then convert V2 to a string PX value.
		function setupBodyMarginValues(){
			if (('number'===typeof(settings.bodyMargin)) || ('0'===settings.bodyMargin)){
				settings.bodyMarginV1 = settings.bodyMargin;
				settings.bodyMargin   = '' + settings.bodyMargin + 'px';
			}
		}

		function createOutgoingMsg(){
			return iframeID +
				':' + settings.bodyMarginV1 +
				':' + settings.sizeWidth +
				':' + settings.log +
				':' + settings.interval +
				':' + settings.enablePublicMethods +
				':' + settings.autoResize +
				':' + settings.bodyMargin +
				':' + settings.heightCalculationMethod +
				':' + settings.bodyBackground +
				':' + settings.bodyPadding +
				':' + settings.tolerance;
		}

		function init(msg){
			//We have to call trigger twice, as we can not be sure if all
			//iframes have completed loading when this code runs. The
			//event listener also catches the page changing in the iFrame.
			addEventListener(iframe,'load',function(){
				var fr = firstRun;   // Reduce scope of var to function, because IE8's JS execution
                                     // context stack is borked and this value gets externally
                                     // changed midway through running this function.
				trigger('iFrame.onload',msg,iframe);
				if (!fr && settings.heightCalculationMethod in resetRequiredMethods){
					resetIFrame({
						iframe:iframe,
						height:0,
						width:0,
						type:'init'
					});
				}
			});
			trigger('init',msg,iframe);
		}

		var
            /*jshint validthis:true */
			iframe   = this,
			iframeID = ensureHasId(iframe.id);

		setScrolling();
		setLimits();
		setupBodyMarginValues();
		init(createOutgoingMsg());
	}

	function checkOptions(options){
		if ('object' !== typeof options){
			throw new TypeError('Options is not an object.');
		}
	}

	function createNativePublicFunction(){
		function init(element){
			if('IFRAME' !== element.tagName.toUpperCase()) {
				throw new TypeError('Expected <IFRAME> tag, found <'+element.tagName+'>.');
			} else {
				setupIFrame.call(element);
			}
		}

		function processOptions(options){
			options = options || {};

			checkOptions(options);

			for (var option in defaults) {
				if (defaults.hasOwnProperty(option)){
					settings[option] = options.hasOwnProperty(option) ? options[option] : defaults[option];
				}
			}
		}

		return function iFrameResizeF(options,selecter){
			processOptions(options);
			Array.prototype.forEach.call( document.querySelectorAll( selecter || 'iframe' ), init );
		};
	}

	function createJQueryPublicMethod($){
		$.fn.iFrameResize = function $iFrameResizeF(options) {
			options = options || {};
			checkOptions(options);
			settings = $.extend( {}, defaults, options );
			return this.filter('iframe').each( setupIFrame ).end();
		};
	}

	setupRequestAnimationFrame();
	addEventListener(window,'message',iFrameListener);

	if (window.jQuery) { createJQueryPublicMethod(jQuery); }

	if (typeof define === 'function' && define.amd) {
		define(function (){ return createNativePublicFunction(); });
	} else {
		window.iFrameResize = createNativePublicFunction();
	}

})();
