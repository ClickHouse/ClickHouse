
const Icon = () => {
    return (
        <div className="privatePreviewIcon">
            <svg width="16" height="16" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
                <path d="M5.33301 6.66667V4.66667V4.66667C5.33301 3.194 6.52701 2 7.99967 2V2C9.47234 2 10.6663 3.194 10.6663 4.66667V4.66667V6.66667" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round"/>
                <path d="M8.00033 9.33337V11.3334" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round"/>
                <path fillRule="evenodd" clipRule="evenodd" d="M11.333 14H4.66634C3.92967 14 3.33301 13.4033 3.33301 12.6666V7.99996C3.33301 7.26329 3.92967 6.66663 4.66634 6.66663H11.333C12.0697 6.66663 12.6663 7.26329 12.6663 7.99996V12.6666C12.6663 13.4033 12.0697 14 11.333 14Z" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round"/>
            </svg>
        </div>
    )
}

export const PrivatePreviewBadge = () => {
    return (
        <div className="privatePreviewBadge">
            <Icon />
            {'Private preview in ClickHouse Cloud'}
        </div>
    )
}
